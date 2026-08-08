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

#include <algorithm>
#include <cctype>
#include <cstddef>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <functional>
#include <initializer_list>
#include <iostream>
#include <iterator>
#include <memory>
#include <optional>
#include <regex>
#include <ranges>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>
#include <unistd.h>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <DataTypes/SchemaFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <QueryManager/GRPCQuerySubmissionBackend.hpp>
#include <QueryManager/QueryManager.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Operators/Sinks/SinkLogicalOperator.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Statements/StatementHandler.hpp>
#include <Statements/StatementJsonSerializers.hpp> ///NOLINT(misc-include-cleaner)
#include <Statements/StatementOutputAssembler.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/Pointers.hpp>
#include <Util/Signal.hpp>
#include <Util/Strings.hpp>
#include <argparse/argparse.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <rfl/Generic.hpp>
#include <rfl/json/write.hpp>
#include <yaml-cpp/node/node.h>
#include <yaml-cpp/yaml.h> ///NOLINT(misc-include-cleaner)
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <ModelCatalog.hpp>
#include <QueryOptimizer.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <QueryStateBackend.hpp>
#include <Version.hpp>
#include <WorkerCatalog.hpp>

namespace
{
NES::DataType stringToFieldType(const std::string& fieldNodeType)
{
    try
    {
        return NES::DataTypeProvider::provideDataType(fieldNodeType);
    }
    catch (std::runtime_error& e)
    {
        throw NES::SLTWrongSchema("Found invalid logical source configuration. {} is not a proper datatype.", fieldNodeType);
    }
}

NES::Identifier bindIdentifierName(std::string_view identifier)
{
    auto identifierOrError = NES::Identifier::tryParse(std::string{identifier});
    if (!identifierOrError)
    {
        throw std::move(identifierOrError).error();
    }
    return identifierOrError.value();
}
}

namespace NES::CLI
{
/// In CLI SchemaField, Sink, LogicalSource, PhysicalSource and QueryConfig are used as target for the YAML parser.
/// These types should not be used anywhere else in NES; instead we use the bound and validated types, such as LogicalSource and SourceDescriptor.
struct SchemaField
{
    SchemaField(std::string name, const std::string& typeName);
    SchemaField(std::string name, DataType type);
    SchemaField() = default;

    std::string name;
    DataType type;
};

struct Sink
{
    std::string name;
    std::vector<SchemaField> schema;
    std::string type;
    std::string host;
    std::unordered_map<std::string, std::string> config;
    std::unordered_map<std::string, std::string> parserConfig;
};

struct LogicalSource
{
    std::string name;
    std::vector<SchemaField> schema;
};

struct PhysicalSource
{
    std::string logical;
    std::string type;
    std::string host;
    std::unordered_map<std::string, std::string> parserConfig;
    std::unordered_map<std::string, std::string> sourceConfig;
};

struct WorkerConfig
{
    std::string host;
    std::string dataAddress;
    std::optional<size_t> maxOperators;
    std::vector<std::string> downstream;
    std::unordered_map<std::string, std::string> config; /// Flattened dot-separated config (e.g., "worker.receiver_queue_size" -> "2")
};

struct Model
{
    std::string name;
    std::string path;
    std::vector<SchemaField> input;
    std::vector<SchemaField> output;
};

struct NamedQuery
{
    std::optional<std::string> name;
    std::string query;
    bool disabled = false;
};

struct QueryConfig
{
    std::vector<NamedQuery> query;
    std::vector<Sink> sinks;
    std::vector<LogicalSource> logical;
    std::vector<PhysicalSource> physical;
    YAML::Node optimizer;
    std::vector<WorkerConfig> workers;
    std::vector<Model> models;
};
}

namespace
{
thread_local std::vector<std::string> yamlPath;

struct YamlPathGuard
{
    explicit YamlPathGuard(std::string segment) { yamlPath.push_back(std::move(segment)); }

    ~YamlPathGuard() { yamlPath.pop_back(); }

    YamlPathGuard(const YamlPathGuard&) = delete;
    YamlPathGuard& operator=(const YamlPathGuard&) = delete;
    YamlPathGuard(YamlPathGuard&&) = delete;
    YamlPathGuard& operator=(YamlPathGuard&&) = delete;
};

std::string currentYamlPath()
{
    std::string result;
    for (const auto& segment : yamlPath)
    {
        if (!result.empty() && segment[0] != '[')
        {
            result += '.';
        }
        result += segment;
    }
    return result.empty() ? "<root>" : result;
}

std::string formatMark(const YAML::Mark& mark)
{
    if (mark.is_null())
    {
        return "";
    }
    return fmt::format(" (line {})", mark.line + 1);
}

template <typename T>
T getValue(const YAML::Node& node, const std::string& key)
{
    const YamlPathGuard guard(key);
    if (!node[key])
    {
        throw NES::InvalidConfigParameter("Missing required key '{}' at {}{}", key, currentYamlPath(), formatMark(node.Mark()));
    }
    try
    {
        return node[key].as<T>();
    }
    catch (const YAML::Exception&)
    {
        throw NES::InvalidConfigParameter("Invalid value for '{}' at {}{}", key, currentYamlPath(), formatMark(node[key].Mark()));
    }
}

template <typename T>
std::vector<T> getList(const YAML::Node& node, const std::string& key)
{
    const YamlPathGuard guard(key);
    if (!node[key])
    {
        throw NES::InvalidConfigParameter("Missing required key '{}' at {}{}", key, currentYamlPath(), formatMark(node.Mark()));
    }
    if (!node[key].IsSequence())
    {
        throw NES::InvalidConfigParameter("Expected a list at {}{}", currentYamlPath(), formatMark(node[key].Mark()));
    }
    std::vector<T> result;
    for (std::size_t i = 0; i < node[key].size(); ++i)
    {
        const YamlPathGuard indexGuard("[" + std::to_string(i) + "]");
        try
        {
            result.push_back(node[key][i].as<T>());
        }
        catch (const YAML::Exception&)
        {
            throw NES::InvalidConfigParameter("Invalid value at {}{}", currentYamlPath(), formatMark(node[key][i].Mark()));
        }
    }
    return result;
}

template <typename T>
T getOrDefault(const YAML::Node& node, const std::string& key, T defaultValue = T{})
{
    if (!node[key])
    {
        return defaultValue;
    }
    const YamlPathGuard guard(key);
    try
    {
        return node[key].as<T>();
    }
    catch (const YAML::Exception&)
    {
        throw NES::InvalidConfigParameter("Invalid value for '{}' at {}{}", key, currentYamlPath(), formatMark(node[key].Mark()));
    }
}

template <typename T>
std::optional<T> getOptional(const YAML::Node& node, const std::string& key)
{
    if (!node[key])
    {
        return std::nullopt;
    }
    const YamlPathGuard guard(key);
    try
    {
        return node[key].as<T>();
    }
    catch (const YAML::Exception&)
    {
        throw NES::InvalidConfigParameter("Invalid value for '{}' at {}{}", key, currentYamlPath(), formatMark(node[key].Mark()));
    }
}

template <typename T>
std::vector<T> getListOrDefault(const YAML::Node& node, const std::string& key)
{
    if (!node[key])
    {
        return {};
    }
    return getList<T>(node, key);
}

/// Validates that a YAML map node contains only the expected keys. Throws InvalidConfigParameter if an unknown key is found.
void acceptKeys(std::initializer_list<std::string_view> allowed, const YAML::Node& node)
{
    if (!node.IsMap())
    {
        return;
    }
    for (const auto& entry : node)
    {
        const auto key = entry.first.as<std::string>();
        if (std::ranges::find(allowed, key) == allowed.end())
        {
            throw NES::InvalidConfigParameter(
                "Unknown key '{}' at {}{}. Expected one of: {}",
                key,
                currentYamlPath(),
                formatMark(entry.first.Mark()),
                fmt::join(allowed, ", "));
        }
    }
}

}

namespace YAML
{
template <>
struct convert<NES::CLI::SchemaField>
{
    static bool decode(const Node& node, NES::CLI::SchemaField& rhs)
    {
        acceptKeys({"name", "type", "nullable"}, node);
        rhs.name = getValue<std::string>(node, "name");

        const bool nullable = getOrDefault<bool>(node, "nullable", false);
        rhs.type = stringToFieldType(getValue<std::string>(node, "type"));
        rhs.type.nullable = nullable;
        return true;
    }
};

template <>
struct convert<NES::CLI::Sink>
{
    static bool decode(const Node& node, NES::CLI::Sink& rhs)
    {
        acceptKeys({"name", "type", "schema", "host", "config", "parser_config"}, node);
        rhs.name = getValue<std::string>(node, "name");
        rhs.type = getValue<std::string>(node, "type");
        rhs.schema = getList<NES::CLI::SchemaField>(node, "schema");
        rhs.host = getValue<std::string>(node, "host");
        rhs.config = getOrDefault<std::unordered_map<std::string, std::string>>(node, "config");
        rhs.parserConfig = getOrDefault<std::unordered_map<std::string, std::string>>(node, "parser_config");
        return true;
    }
};

template <>
struct convert<NES::CLI::LogicalSource>
{
    static bool decode(const Node& node, NES::CLI::LogicalSource& rhs)
    {
        acceptKeys({"name", "schema"}, node);
        rhs.name = getValue<std::string>(node, "name");
        rhs.schema = getList<NES::CLI::SchemaField>(node, "schema");
        return true;
    }
};

template <>
struct convert<NES::CLI::PhysicalSource>
{
    static bool decode(const Node& node, NES::CLI::PhysicalSource& rhs)
    {
        acceptKeys({"logical", "type", "host", "parser_config", "source_config"}, node);
        rhs.logical = getValue<std::string>(node, "logical");
        rhs.type = getValue<std::string>(node, "type");
        rhs.host = getValue<std::string>(node, "host");
        rhs.parserConfig = getValue<std::unordered_map<std::string, std::string>>(node, "parser_config");
        rhs.sourceConfig = getOrDefault<std::unordered_map<std::string, std::string>>(node, "source_config");
        return true;
    }
};

template <>
struct convert<NES::CLI::WorkerConfig>
{
    static bool decode(const Node& node, NES::CLI::WorkerConfig& rhs)
    {
        acceptKeys({"host", "data_address", "max_operators", "downstream", "config"}, node);
        rhs.maxOperators = getOptional<size_t>(node, "max_operators");
        rhs.downstream = getOrDefault<std::vector<std::string>>(node, "downstream");
        rhs.host = getValue<std::string>(node, "host");
        rhs.dataAddress = getOrDefault<std::string>(node, "data_address");
        return true;
    }
};

template <>
struct convert<NES::CLI::Model>
{
    static bool decode(const Node& node, NES::CLI::Model& rhs)
    {
        rhs.name = fmt::format("{}", bindIdentifierName(node["name"].as<std::string>()));
        rhs.path = node["path"].as<std::string>();
        rhs.input = node["input"].as<std::vector<NES::CLI::SchemaField>>();
        rhs.output = node["output"].as<std::vector<NES::CLI::SchemaField>>();
        return true;
    }
};

template <>
struct convert<NES::CLI::NamedQuery>
{
    static bool decode(const Node& node, NES::CLI::NamedQuery& rhs)
    {
        if (node.IsScalar())
        {
            rhs.name = std::nullopt;
            rhs.query = node.as<std::string>();
            return true;
        }
        else if (node.IsMap())
        {
            acceptKeys({"query", "name", "disabled"}, node);
            rhs.name = getOptional<std::string>(node, "name");
            rhs.query = node["query"].as<std::string>();
            rhs.disabled = getOrDefault<bool>(node, "disabled", false);
            return true;
        }
        return false;
    }
};

template <>
struct convert<NES::CLI::QueryConfig>
{
    static bool decode(const Node& node, NES::CLI::QueryConfig& rhs)
    {
        acceptKeys({"query", "sinks", "logical", "physical", "optimizer", "workers", "models"}, node);
        rhs.sinks = getListOrDefault<NES::CLI::Sink>(node, "sinks");
        rhs.logical = getList<NES::CLI::LogicalSource>(node, "logical");
        rhs.physical = getListOrDefault<NES::CLI::PhysicalSource>(node, "physical");

        if (node["optimizer"].IsDefined())
        {
            rhs.optimizer = node["optimizer"];
        }
        rhs.models = {};
        if (node["models"].IsDefined())
        {
            rhs.models = node["models"].as<std::vector<NES::CLI::Model>>();
        }
        rhs.query = {};
        if (node["query"].IsDefined())
        {
            if (node["query"].IsSequence())
            {
                rhs.query = node["query"].as<std::vector<NES::CLI::NamedQuery>>();
            }
            else
            {
                rhs.query.emplace_back(node["query"].as<NES::CLI::NamedQuery>());
            }
        }
        rhs.workers = getList<NES::CLI::WorkerConfig>(node, "workers");
        return true;
    }
};
}

namespace
{
NES::CLI::QueryConfig getTopologyPath(const argparse::ArgumentParser& parser)
{
    /// Check -t flag first
    if (parser.is_used("-t"))
    {
        const auto filePath = parser.get<std::string>("-t");

        /// Read topology from stdin
        if (filePath == "-")
        {
            if (isatty(STDIN_FILENO) != 0)
            {
                throw NES::InvalidConfigParameter("Cannot read topology from stdin: stdin is a terminal");
            }
            try
            {
                std::stringstream buffer;
                buffer << std::cin.rdbuf();
                const std::string yamlContent = buffer.str();
                if (yamlContent.empty())
                {
                    throw NES::InvalidConfigParameter("No topology data received from stdin");
                }
                auto validYAML = YAML::Load(yamlContent);
                NES_DEBUG("Using topology from stdin");
                return validYAML.as<NES::CLI::QueryConfig>();
            }
            catch (YAML::Exception& e)
            {
                throw NES::InvalidConfigParameter("stdin is not a valid yaml: {} ({}:{})", e.what(), e.mark.line, e.mark.column);
            }
        }

        if (!std::filesystem::exists(filePath))
        {
            throw NES::InvalidConfigParameter("Topology file specified with -t does not exist: {}", filePath);
        }
        try
        {
            auto validYAML = YAML::LoadFile(filePath);
            NES_DEBUG("Using topology file: {}", filePath);
            return validYAML.as<NES::CLI::QueryConfig>();
        }
        catch (YAML::Exception& e)
        {
            throw NES::InvalidConfigParameter("{} is not a valid yaml file: {} ({}:{})", filePath, e.what(), e.mark.line, e.mark.column);
        }
    }

    std::vector<std::string> options;
    ///NOLINTNEXTLINE(concurrency-mt-unsafe) This is only used at the start of the program on a single thread.
    if (auto* const env = std::getenv("NES_TOPOLOGY_FILE"))
    {
        options.emplace_back(env);
    }
    options.emplace_back("topology.yaml");
    options.emplace_back("topology.yml");

    for (const auto& option : options)
    {
        if (!std::filesystem::exists(option))
        {
            continue;
        }
        try
        {
            /// is valid yaml
            auto validYAML = YAML::LoadFile(option);
            NES_DEBUG("Using topology file: {}", option);
            return validYAML.as<NES::CLI::QueryConfig>();
        }
        catch (YAML::Exception& e)
        {
            /// That wasn't a valid yaml file
            NES_WARNING("{} is not a valid yaml file: {} ({}:{})", option, e.what(), e.mark.line, e.mark.column);
        }
    }
    throw NES::InvalidConfigParameter("Could not find topology file");
}

std::vector<NES::CLI::NamedQuery> loadQueries(
    const argparse::ArgumentParser& /*parser*/, const argparse::ArgumentParser& subcommand, const NES::CLI::QueryConfig& topologyConfig)
{
    std::vector<NES::CLI::NamedQuery> queries;
    if (subcommand.is_used("queries"))
    {
        for (const auto& query : subcommand.get<std::vector<std::string>>("queries"))
        {
            queries.emplace_back(std::nullopt, query);
        }
        NES_DEBUG("loaded {} queries from commandline", queries.size());
    }
    else
    {
        for (const auto& query : topologyConfig.query)
        {
            if (!query.disabled)
            {
                queries.emplace_back(query);
            }
        }
        NES_DEBUG("loaded {} queries from topology file", queries.size());
    }
    return queries;
}

enum class PipeEndpointType : uint8_t
{
    Source,
    Sink
};

struct PipeEndpoint
{
    PipeEndpointType type;
    NES::Identifier name;
    std::optional<std::string> host;
    size_t offset;
    size_t length;
};

struct PipeQueryMetadata
{
    std::vector<PipeEndpoint> endpoints;
};

struct ResolvedPipe
{
    NES::Host host;
    NES::Schema<NES::UnqualifiedUnboundField, NES::Ordered> schema;
};

std::string previousToken(std::string_view query, size_t offset)
{
    while (offset > 0 && std::isspace(static_cast<unsigned char>(query[offset - 1])))
    {
        --offset;
    }
    const auto tokenEnd = offset;
    while (offset > 0)
    {
        const auto character = static_cast<unsigned char>(query[offset - 1]);
        if (!std::isalnum(character) && character != '_')
        {
            break;
        }
        --offset;
    }
    return NES::toUpperCase(std::string(query.substr(offset, tokenEnd - offset)));
}

PipeQueryMetadata findPipeEndpoints(const NES::CLI::NamedQuery& query)
{
    /// Frontend-only shorthand. A sink is PIPE(name, "host"), while a source is PIPE(name).
    /// PIPE is intentionally found independent of its surrounding relation syntax so FROM, JOIN, and nested table expressions work.
    static const std::regex pipePattern{
        R"pipe(\bPIPE\s*\(\s*((?:[a-zA-Z_][a-zA-Z0-9_]*)|(?:"[^"]*"))\s*(?:,\s*"([a-zA-Z0-9:]*)")?\s*\))pipe",
        std::regex_constants::icase};

    PipeQueryMetadata metadata;
    for (auto iter = std::sregex_iterator(query.query.begin(), query.query.end(), pipePattern); iter != std::sregex_iterator{}; ++iter)
    {
        const auto& match = *iter;
        const auto offset = static_cast<size_t>(match.position());
        const auto isSink = previousToken(query.query, offset) == "INTO";
        auto name = bindIdentifierName(match[1].str());
        const auto host = match[2].matched ? std::optional<std::string>{match[2].str()} : std::nullopt;
        if (isSink && (!host.has_value() || host->empty()))
        {
            throw NES::InvalidConfigParameter("Pipe sink '{}' must specify a host", match[1].str());
        }
        if (!isSink && host.has_value())
        {
            throw NES::InvalidConfigParameter(
                "Pipe source '{}' must not specify a host; its host is derived from the producing sink", match[1].str());
        }
        metadata.endpoints.emplace_back(PipeEndpoint{
            .type = isSink ? PipeEndpointType::Sink : PipeEndpointType::Source,
            .name = std::move(name),
            .host = host,
            .offset = offset,
            .length = static_cast<size_t>(match.length())});
    }
    return metadata;
}

std::vector<size_t> pipeQueryOrder(const std::vector<NES::CLI::NamedQuery>& queries, const std::vector<PipeQueryMetadata>& metadata)
{
    std::unordered_map<NES::Identifier, size_t> producers;
    for (size_t queryIndex = 0; queryIndex < metadata.size(); ++queryIndex)
    {
        for (const auto& endpoint : metadata[queryIndex].endpoints)
        {
            if (endpoint.type != PipeEndpointType::Sink)
            {
                continue;
            }
            const auto [existing, inserted] = producers.emplace(endpoint.name, queryIndex);
            if (!inserted)
            {
                throw NES::InvalidConfigParameter(
                    "Pipe '{}' has multiple producers (queries {} and {})",
                    endpoint.name.getOriginalString(),
                    existing->second,
                    queryIndex);
            }
        }
    }

    std::vector<std::set<size_t>> successors(queries.size());
    std::vector<size_t> predecessorCount(queries.size(), 0);
    for (size_t consumerIndex = 0; consumerIndex < metadata.size(); ++consumerIndex)
    {
        for (const auto& endpoint : metadata[consumerIndex].endpoints)
        {
            if (endpoint.type != PipeEndpointType::Source)
            {
                continue;
            }
            const auto producer = producers.find(endpoint.name);
            if (producer == producers.end())
            {
                throw NES::InvalidConfigParameter(
                    "Pipe source '{}' has no producer in this submission", endpoint.name.getOriginalString());
            }
            if (successors[producer->second].insert(consumerIndex).second)
            {
                ++predecessorCount[consumerIndex];
            }
        }
    }

    /// A set makes the topological order deterministic and preserves declaration order whenever dependencies permit it.
    std::set<size_t> ready;
    for (size_t queryIndex = 0; queryIndex < predecessorCount.size(); ++queryIndex)
    {
        if (predecessorCount[queryIndex] == 0)
        {
            ready.insert(queryIndex);
        }
    }

    std::vector<size_t> order;
    order.reserve(queries.size());
    while (!ready.empty())
    {
        const auto queryIndex = *ready.begin();
        ready.erase(ready.begin());
        order.push_back(queryIndex);
        for (const auto successor : successors[queryIndex])
        {
            if (--predecessorCount[successor] == 0)
            {
                ready.insert(successor);
            }
        }
    }
    if (order.size() != queries.size())
    {
        throw NES::InvalidConfigParameter("Pipe query dependencies contain a cycle");
    }
    return order;
}

std::set<size_t> ignoredPipeQueries(const std::vector<NES::CLI::NamedQuery>& queries, const std::vector<PipeQueryMetadata>& metadata)
{
    std::unordered_map<NES::Identifier, size_t> producers;
    for (size_t queryIndex = 0; queryIndex < metadata.size(); ++queryIndex)
    {
        for (const auto& endpoint : metadata[queryIndex].endpoints)
        {
            if (endpoint.type == PipeEndpointType::Sink)
            {
                producers.emplace(endpoint.name, queryIndex);
            }
        }
    }

    /// Non-pipe sinks are terminal outputs. Walk backwards from them to retain only pipe producers that contribute to a terminal output.
    std::vector<bool> required(queries.size(), false);
    std::vector<size_t> pending;
    for (size_t queryIndex = 0; queryIndex < metadata.size(); ++queryIndex)
    {
        const auto producesPipe = std::ranges::any_of(
            metadata[queryIndex].endpoints, [](const PipeEndpoint& endpoint) { return endpoint.type == PipeEndpointType::Sink; });
        if (!producesPipe)
        {
            required[queryIndex] = true;
            pending.push_back(queryIndex);
        }
    }
    while (!pending.empty())
    {
        const auto queryIndex = pending.back();
        pending.pop_back();
        for (const auto& endpoint : metadata[queryIndex].endpoints)
        {
            if (endpoint.type != PipeEndpointType::Source)
            {
                continue;
            }
            const auto producerIndex = producers.at(endpoint.name);
            if (!required[producerIndex])
            {
                required[producerIndex] = true;
                pending.push_back(producerIndex);
            }
        }
    }

    std::set<size_t> ignored;
    for (size_t queryIndex = 0; queryIndex < required.size(); ++queryIndex)
    {
        if (!required[queryIndex])
        {
            ignored.insert(queryIndex);
        }
    }
    return ignored;
}

std::string pipeDependencyOutput(
    const std::vector<NES::CLI::NamedQuery>& queries,
    const std::vector<PipeQueryMetadata>& metadata,
    const std::set<size_t>& ignoredQueries)
{
    struct Producer
    {
        size_t queryIndex;
        std::string host;
    };
    std::unordered_map<NES::Identifier, Producer> producers;
    for (size_t queryIndex = 0; queryIndex < metadata.size(); ++queryIndex)
    {
        for (const auto& endpoint : metadata[queryIndex].endpoints)
        {
            if (endpoint.type == PipeEndpointType::Sink)
            {
                producers.emplace(endpoint.name, Producer{.queryIndex = queryIndex, .host = endpoint.host.value()});
            }
        }
    }

    const auto queryName = [&queries](size_t queryIndex)
    { return queries[queryIndex].name.value_or(fmt::format("query[{}]", queryIndex)); };

    std::stringstream output;
    fmt::println(output, "== Query Dependencies ==");
    bool hasDependencies = false;
    std::set<std::tuple<size_t, size_t, std::string>> emitted;
    for (size_t consumerIndex = 0; consumerIndex < metadata.size(); ++consumerIndex)
    {
        if (ignoredQueries.contains(consumerIndex))
        {
            continue;
        }
        for (const auto& endpoint : metadata[consumerIndex].endpoints)
        {
            if (endpoint.type != PipeEndpointType::Source)
            {
                continue;
            }
            const auto& producer = producers.at(endpoint.name);
            if (ignoredQueries.contains(producer.queryIndex))
            {
                continue;
            }
            if (!emitted.emplace(producer.queryIndex, consumerIndex, endpoint.name.asCanonicalString()).second)
            {
                continue;
            }
            fmt::println(
                output,
                "{} --[{}@{}]--> {}",
                queryName(producer.queryIndex),
                endpoint.name.getOriginalString(),
                producer.host,
                queryName(consumerIndex));
            hasDependencies = true;
        }
    }
    if (!hasDependencies)
    {
        fmt::println(output, "(none)");
    }

    fmt::println(output, "\n== Ignored Pipe Queries ==");
    if (ignoredQueries.empty())
    {
        fmt::println(output, "(none)");
        return output.str();
    }

    std::unordered_map<NES::Identifier, std::vector<size_t>> consumers;
    for (size_t queryIndex = 0; queryIndex < metadata.size(); ++queryIndex)
    {
        for (const auto& endpoint : metadata[queryIndex].endpoints)
        {
            if (endpoint.type == PipeEndpointType::Source)
            {
                consumers[endpoint.name].push_back(queryIndex);
            }
        }
    }
    for (const auto queryIndex : ignoredQueries)
    {
        for (const auto& endpoint : metadata[queryIndex].endpoints)
        {
            if (endpoint.type != PipeEndpointType::Sink)
            {
                continue;
            }
            const auto pipeConsumers = consumers[endpoint.name];
            if (pipeConsumers.empty())
            {
                fmt::println(
                    output,
                    "{} ({}@{} is not consumed)",
                    queryName(queryIndex),
                    endpoint.name.getOriginalString(),
                    endpoint.host.value());
                continue;
            }
            const auto consumerNames = pipeConsumers
                | std::views::transform([&queryName](size_t consumerIndex) { return queryName(consumerIndex); })
                | std::ranges::to<std::vector>();
            fmt::println(
                output,
                "{} ({}@{} is consumed only by ignored queries: {})",
                queryName(queryIndex),
                endpoint.name.getOriginalString(),
                endpoint.host.value(),
                fmt::join(consumerNames, ", "));
        }
    }
    return output.str();
}

std::string schemaToSQL(const NES::Schema<NES::UnqualifiedUnboundField, NES::Ordered>& schema)
{
    std::vector<std::string> fields;
    fields.reserve(schema.size());
    for (const auto& field : schema)
    {
        const NES::Identifier& fieldName = field.getFullyQualifiedName();
        const auto canonicalName = fieldName.asCanonicalString();
        if (canonicalName.contains('"'))
        {
            throw NES::InvalidConfigParameter("Cannot insert pipe schema field containing a quote into SQL: {}", canonicalName);
        }
        const auto typeName = magic_enum::enum_name(field.getDataType().type);
        if (typeName.empty() || field.getDataType().type == NES::DataType::Type::UNDEFINED)
        {
            throw NES::InvalidConfigParameter("Cannot insert undefined pipe schema field '{}' into SQL", canonicalName);
        }
        fields.emplace_back(fmt::format("\"{}\" {}{}", canonicalName, typeName, field.getDataType().nullable ? "" : " NOT NULL"));
    }
    return fmt::format("SCHEMA({})", fmt::join(fields, ", "));
}

std::string rewritePipeQuery(
    const NES::CLI::NamedQuery& query,
    const PipeQueryMetadata& metadata,
    const std::unordered_map<NES::Identifier, ResolvedPipe>& resolvedPipes)
{
    std::string rewritten = query.query;
    for (auto endpoint = metadata.endpoints.rbegin(); endpoint != metadata.endpoints.rend(); ++endpoint)
    {
        std::string replacement;
        if (endpoint->type == PipeEndpointType::Sink)
        {
            replacement = fmt::format(
                "Pipe('{}' AS \"SINK\".\"PIPE_NAME\", '{}' AS \"SINK\".\"HOST\")",
                endpoint->name.asCanonicalString(),
                endpoint->host.value());
        }
        else
        {
            const auto resolved = resolvedPipes.find(endpoint->name);
            if (resolved == resolvedPipes.end())
            {
                throw NES::InvalidConfigParameter(
                    "Pipe source '{}' was reached before its producer was resolved", endpoint->name.getOriginalString());
            }
            replacement = fmt::format(
                "Pipe('{}' AS \"SOURCE\".\"PIPE_NAME\", '{}' AS \"SOURCE\".\"HOST\", "
                "'NATIVE' AS \"INPUT_FORMATTER\".\"TYPE\", {} AS \"SOURCE\".\"SCHEMA\")",
                endpoint->name.asCanonicalString(),
                resolved->second.host.getRawValue(),
                schemaToSQL(resolved->second.schema));
        }
        rewritten.replace(endpoint->offset, endpoint->length, replacement);
    }
    return rewritten;
}

void resolvePipeSink(
    const PipeQueryMetadata& metadata,
    const NES::DistributedLogicalPlan& distributedPlan,
    std::unordered_map<NES::Identifier, ResolvedPipe>& resolvedPipes)
{
    const auto sinks = metadata.endpoints
        | std::views::filter([](const PipeEndpoint& endpoint) { return endpoint.type == PipeEndpointType::Sink; })
        | std::ranges::to<std::vector>();
    if (sinks.empty())
    {
        return;
    }
    if (sinks.size() != 1)
    {
        throw NES::InvalidConfigParameter("A query may currently produce only one pipe");
    }

    const auto& roots = distributedPlan.getGlobalPlan().getRootOperators();
    if (roots.size() != 1)
    {
        throw NES::InvalidConfigParameter("Expected a pipe-producing query to have exactly one sink");
    }
    const auto sinkOperator = roots.front().tryGetAs<NES::SinkLogicalOperator>();
    if (!sinkOperator.has_value() || !sinkOperator.value()->getSinkDescriptor().has_value())
    {
        throw NES::InvalidConfigParameter("Optimized pipe-producing query has no sink descriptor");
    }
    const auto descriptor = sinkOperator.value()->getSinkDescriptor().value();
    if (NES::toUpperCase(descriptor.getSinkType()) != "PIPE")
    {
        throw NES::InvalidConfigParameter("Expected PIPE sink but optimizer produced sink type '{}'", descriptor.getSinkType());
    }
    const auto schema = descriptor.getSchema();
    const auto* orderedSchema = std::get_if<std::shared_ptr<const NES::Schema<NES::UnqualifiedUnboundField, NES::Ordered>>>(&schema);
    if (orderedSchema == nullptr)
    {
        throw NES::InvalidConfigParameter(
            "Optimizer did not infer an ordered schema for pipe '{}'", sinks.front().name.getOriginalString());
    }
    resolvedPipes.emplace(sinks.front().name, ResolvedPipe{.host = descriptor.getHost(), .schema = **orderedSchema});
}

std::unordered_map<NES::Identifier, std::string> bindConfig(const std::unordered_map<std::string, std::string>& config)
{
    const auto boundConfig = config
        | std::views::transform([](const auto& rawPair) { return std::make_pair(bindIdentifierName(rawPair.first), rawPair.second); })
        | std::ranges::to<std::unordered_map<NES::Identifier, std::string>>();
    if (std::ranges::size(config) != std::ranges::size(boundConfig))
    {
        throw NES::InvalidConfigParameter("Duplicate parameters with different casing");
    }
    return boundConfig;
}

NES::Schema<NES::UnqualifiedUnboundField, NES::Ordered> bindSchema(const std::vector<NES::CLI::SchemaField>& schemaFields)
{
    const auto boundSchema = NES::Schema<NES::UnqualifiedUnboundField, NES::Ordered>::tryCreateCollisionFree(
        schemaFields
        | std::views::transform([](const auto& rawField)
                                { return NES::UnqualifiedUnboundField{bindIdentifierName(rawField.name), rawField.type}; })
        | std::ranges::to<std::vector>());
    if (!boundSchema.has_value())
    {
        throw NES::FieldAlreadyExists(NES::Schema<NES::UnqualifiedUnboundField, NES::Ordered>::createCollisionString(boundSchema.error()));
    }
    return std::move(boundSchema).value();
}

std::vector<NES::Statement> loadStatements(const NES::CLI::QueryConfig& topologyConfig)
{
    const auto& [query, sinks, logical, physical, optimizer, workers, models] = topologyConfig;
    std::vector<NES::Statement> statements;
    statements.reserve(workers.size());
    for (const auto& [host, dataAddress, maxOperators, downstream, config] : workers)
    {
        statements.emplace_back(
            NES::CreateWorkerStatement{
                .host = host, .dataAddress = dataAddress, .capacity = maxOperators, .downstream = downstream, .config = config});
    }
    for (const auto& [name, schemaFields] : logical)
    {
        statements.emplace_back(NES::CreateLogicalSourceStatement{.name = bindIdentifierName(name), .schema = bindSchema(schemaFields)});
    }

    for (const auto& [logical, type, host, parserConfig, sourceConfig] : physical)
    {
        statements.emplace_back(
            NES::CreatePhysicalSourceStatement{
                .attachedTo = bindIdentifierName(logical),
                .sourceType = bindIdentifierName(type),
                .host = NES::Host(host),
                .sourceConfig = bindConfig(sourceConfig),
                .parserConfig = bindConfig(parserConfig)});
    }
    for (const auto& [name, schemaFields, type, host, config, parserConfig] : sinks)
    {
        statements.emplace_back(
            NES::CreateSinkStatement{
                .name = bindIdentifierName(name),
                .sinkType = bindIdentifierName(type),
                .schema = bindSchema(schemaFields),
                .host = NES::Host(host),
                .sinkConfig = bindConfig(config),
                .formatConfig = bindConfig(parserConfig)});
    }
    for (const auto& [name, path, input, output] : models)
    {
        const auto toModelFields = [](const std::vector<NES::CLI::SchemaField>& fields)
        {
            return fields
                | std::views::transform(
                       [](const NES::CLI::SchemaField& field)
                       {
                           auto fieldNameExpected = NES::Identifier::tryParse(field.name);
                           if (!fieldNameExpected.has_value())
                           {
                               throw std::move(fieldNameExpected.error());
                           }
                           return NES::UnqualifiedUnboundField{fieldNameExpected.value(), field.type};
                       })
                | std::ranges::to<NES::Schema<NES::UnqualifiedUnboundField, NES::Ordered>>();
        };
        statements.emplace_back(
            NES::CreateModelStatement{.name = name, .path = path, .inputs = toModelFields(input), .outputs = toModelFields(output)});
    }
    return statements;
}

NES::QueryOptimizerConfiguration loadQueryOptimizerConfiguration(const NES::CLI::QueryConfig& topologyConfig)
{
    NES::QueryOptimizerConfiguration configuration;
    if (topologyConfig.optimizer.IsDefined())
    {
        configuration.overwriteConfigWithYAMLNode(topologyConfig.optimizer);
    }
    return configuration;
}

void doStatus(
    NES::QueryStatementHandler& queryStatementHandler,
    NES::TopologyStatementHandler& topologyStatementHandler,
    const std::vector<NES::DistributedQueryId>& queries)
{
    if (queries.empty())
    {
        auto result = topologyStatementHandler(NES::WorkerStatusStatement{{}});
        if (!result)
        {
            throw std::move(result.error());
        }
        auto rows = NES::rowsToJsonArray(
            NES::StatementOutputAssembler<NES::WorkerStatusStatementResult>{}.convert(result.value()), NES::ReflectionContext{});
        std::cout << rfl::json::write(rows, rfl::json::pretty) << '\n';
    }
    else
    {
        rfl::Generic::Array result;
        for (const auto& query : queries)
        {
            auto statementResult
                = queryStatementHandler(NES::ShowQueriesStatement{.id = query, .format = NES::StatementOutputFormat::JSON});
            if (!statementResult)
            {
                throw std::move(statementResult.error());
            }

            auto results = NES::rowsToJsonArray(
                NES::StatementOutputAssembler<NES::ShowQueriesStatementResult>{}.convert(statementResult.value()),
                NES::ReflectionContext{});
            result.insert(result.end(), std::make_move_iterator(results.begin()), std::make_move_iterator(results.end()));
        }

        std::cout << rfl::json::write(result, rfl::json::pretty) << '\n';
    }
}

void doStop(NES::QueryStatementHandler& queryStatementHandler, const std::vector<NES::DistributedQueryId>& queries)
{
    rfl::Generic::Array result;
    for (const auto& query : queries)
    {
        auto statementResult = queryStatementHandler(NES::DropQueryStatement{.id = query});
        if (!statementResult)
        {
            throw std::move(statementResult.error());
        }

        auto results = NES::rowsToJsonArray(
            NES::StatementOutputAssembler<NES::DropQueryStatementResult>{}.convert(statementResult.value()), NES::ReflectionContext{});
        result.insert(result.end(), std::make_move_iterator(results.begin()), std::make_move_iterator(results.end()));
    }

    std::cout << rfl::json::write(result, rfl::json::pretty) << '\n';
}

void doQueryManagement(const argparse::ArgumentParser& program, const argparse::ArgumentParser& subcommand)
{
    const auto topologyConfig = getTopologyPath(program);
    auto queryOptimizationConfiguration = loadQueryOptimizerConfiguration(topologyConfig);
    NES::CLI::QueryStateBackend stateBackend;

    std::unordered_map<NES::DistributedQueryId, NES::DistributedQuery> state;
    if (subcommand.is_used("queryId"))
    {
        state = subcommand.get<std::vector<std::string>>("queryId")
            | std::views::transform(
                    [&stateBackend](const std::string& queryId) -> std::pair<NES::DistributedQueryId, NES::DistributedQuery>
                    {
                        auto persistedId = NES::CLI::PersistedQueryId::fromString(queryId);
                        auto distributedQuery = stateBackend.load(persistedId);
                        return {persistedId.queryId, distributedQuery};
                    })
            | std::ranges::to<std::unordered_map>();
    }
    else
    {
        state = topologyConfig.query
            | std::views::filter([](const auto& namedQuery) { return namedQuery.name.has_value() && !namedQuery.disabled; })
            | std::views::transform([](const auto& namedQuery) { return namedQuery.name.value(); })
            | std::views::transform(
                    [&stateBackend](const std::string& queryId) -> std::pair<NES::DistributedQueryId, NES::DistributedQuery>
                    {
                        auto persistedId = NES::CLI::PersistedQueryId::fromString(queryId);
                        auto distributedQuery = stateBackend.load(persistedId);
                        return {persistedId.queryId, distributedQuery};
                    })
            | std::ranges::to<std::unordered_map>();
    }

    if (state.empty())
    {
        throw NES::InvalidConfigParameter(
            "QueryManagement subcommand requires at least one queryId passed as CLI argument or a named query in the topology file");
    }

    const auto queries = state | std::views::keys | std::ranges::to<std::vector>();

    auto workerCatalog = std::make_shared<NES::WorkerCatalog>();
    auto sourceCatalog = std::make_shared<NES::SourceCatalog>();
    auto sinkCatalog = std::make_shared<NES::SinkCatalog>();
    auto modelCatalog = std::make_shared<NES::ModelCatalog>();
    const auto queryManager = std::make_shared<NES::QueryManager>(workerCatalog, NES::createGRPCBackend(), NES::QueryManagerState{state});

    NES::TopologyStatementHandler topologyHandler{queryManager, workerCatalog};
    NES::SourceStatementHandler sourceHandler{sourceCatalog, NES::RequireHostConfig{}};
    NES::SinkStatementHandler sinkHandler{sinkCatalog, NES::RequireHostConfig{}};
    NES::ModelStatementHandler modelHandler{modelCatalog};
    auto queryOptimizer
        = std::make_shared<NES::QueryOptimizer>(queryOptimizationConfiguration, sourceCatalog, sinkCatalog, workerCatalog, modelCatalog);
    NES::QueryStatementHandler queryHandler{queryManager, queryOptimizer};

    handleStatements(loadStatements(topologyConfig), topologyHandler, sourceHandler, sinkHandler, modelHandler);

    if (program.is_subcommand_used("stop"))
    {
        doStop(queryHandler, queries);
    }
    else if (program.is_subcommand_used("status"))
    {
        doStatus(queryHandler, topologyHandler, queries);
    }
    else
    {
        throw NES::InvalidConfigParameter("Invalid query management subcommand");
    }
}

void doQuerySubmission(const argparse::ArgumentParser& program, const argparse::ArgumentParser& subcommand)
{
    auto topologyConfig = getTopologyPath(program);
    auto statements = loadStatements(topologyConfig);
    auto queries = loadQueries(program, subcommand, topologyConfig);
    auto queryOptimizerConfiguration = loadQueryOptimizerConfiguration(topologyConfig);
    if (queries.empty())
    {
        throw NES::InvalidConfigParameter("No queries");
    }
    const auto pipeMetadata = queries | std::views::transform(findPipeEndpoints) | std::ranges::to<std::vector<PipeQueryMetadata>>();
    const auto allQueryOrder = pipeQueryOrder(queries, pipeMetadata);
    const auto ignoredQueries = ignoredPipeQueries(queries, pipeMetadata);
    const auto queryOrder = allQueryOrder
        | std::views::filter([&ignoredQueries](size_t queryIndex) { return !ignoredQueries.contains(queryIndex); })
        | std::ranges::to<std::vector>();

    auto workerCatalog = std::make_shared<NES::WorkerCatalog>();
    auto sourceCatalog = std::make_shared<NES::SourceCatalog>();
    auto sinkCatalog = std::make_shared<NES::SinkCatalog>();
    auto modelCatalog = std::make_shared<NES::ModelCatalog>();
    auto queryManager = std::make_shared<NES::QueryManager>(workerCatalog, NES::createGRPCBackend());

    NES::TopologyStatementHandler topologyHandler{queryManager, workerCatalog};
    NES::SourceStatementHandler sourceHandler{sourceCatalog, NES::RequireHostConfig{}};
    NES::SinkStatementHandler sinkHandler{sinkCatalog, NES::RequireHostConfig{}};
    NES::ModelStatementHandler modelHandler{modelCatalog};
    auto queryOptimizer
        = std::make_shared<NES::QueryOptimizer>(queryOptimizerConfiguration, sourceCatalog, sinkCatalog, workerCatalog, modelCatalog);
    handleStatements(statements, topologyHandler, sourceHandler, sinkHandler, modelHandler);

    std::unordered_map<NES::Identifier, ResolvedPipe> resolvedPipes;

    if (program.is_subcommand_used("start"))
    {
        NES::CLI::QueryStateBackend stateBackend;
        for (const auto queryIndex : queryOrder)
        {
            const auto& namedQuery = queries[queryIndex];
            const auto rewrittenQuery = rewritePipeQuery(namedQuery, pipeMetadata[queryIndex], resolvedPipes);
            auto plan = NES::AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(rewrittenQuery);
            auto distributedPlan = queryOptimizer->optimize(std::move(plan));
            auto id = namedQuery.name.transform([](const auto& name) { return NES::DistributedQueryId(name); });
            if (id.has_value())
            {
                distributedPlan.setQueryId(*id);
            }
            resolvePipeSink(pipeMetadata[queryIndex], distributedPlan, resolvedPipes);
            auto result = queryManager->start(distributedPlan);
            if (result)
            {
                auto queryDescriptor = queryManager->getQuery(*result);
                INVARIANT(queryDescriptor.has_value(), "Query should exist in the query manager after a successful start");
                auto persistedId = stateBackend.store(*result, *queryDescriptor);
                std::cout << persistedId.toString() << '\n';
            }
            else
            {
                throw NES::QueryStartFailed(
                    "Could not start query: {}",
                    fmt::join(std::views::transform(result.error(), [](const auto& exception) { return exception.what(); }), ", "));
            }
        }
    }
    else
    {
        std::cout << pipeDependencyOutput(queries, pipeMetadata, ignoredQueries) << '\n';
        for (const auto queryIndex : queryOrder)
        {
            const auto& namedQuery = queries[queryIndex];
            const auto rewrittenQuery = rewritePipeQuery(namedQuery, pipeMetadata[queryIndex], resolvedPipes);
            auto plan = NES::AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(rewrittenQuery);
            const auto distributedPlan = queryOptimizer->optimize(plan);
            resolvePipeSink(pipeMetadata[queryIndex], distributedPlan, resolvedPipes);
            const auto explainString = NES::computeExplainOutput(NES::ExplainQueryStatement(std::move(plan)), *queryOptimizer);
            if (namedQuery.name.has_value())
            {
                std::cout << namedQuery.name.value() << ": ";
            }
            std::cout << explainString << "\n";
        }
    }
}
}

int main(int argc, char** argv)
{
    if (NES::hasVersionFlag(argc, argv))
    {
        NES::printVersion("nes-cli");
        return 0;
    }
    CPPTRACE_TRY
    {
        NES::setupSignalHandlers();
        using argparse::ArgumentParser;
        ArgumentParser program("nebucli");
        program.add_argument("-d", "--debug").flag().help("Dump the query plan and enable debug logging");
        program.add_argument("-t").help(
            "Path to the topology file, or '-' to read from stdin. "
            "Resolution order: 1) -t flag, 2) NES_TOPOLOGY_FILE env, 3) topology.yaml/topology.yml in working directory");

        ArgumentParser startQuery("start");
        startQuery.add_argument("queries").nargs(argparse::nargs_pattern::any);

        ArgumentParser stopQuery("stop");
        stopQuery.add_argument("queryId").nargs(argparse::nargs_pattern::any);

        ArgumentParser statusQuery("status");
        statusQuery.add_argument("queryId").nargs(argparse::nargs_pattern::any);

        ArgumentParser dump("dump");
        dump.add_argument("queries").nargs(argparse::nargs_pattern::any);

        program.add_subparser(startQuery);
        program.add_subparser(stopQuery);
        program.add_subparser(statusQuery);
        program.add_subparser(dump);

        std::vector<std::reference_wrapper<ArgumentParser>> queryManagementSubcommands{stopQuery, statusQuery};
        std::vector<std::reference_wrapper<ArgumentParser>> querySubmissionCommands{startQuery, dump};

        try
        {
            program.parse_args(argc, argv);
        }
        catch (const std::exception& e)
        {
            std::cerr << e.what() << "\n";
            std::cerr << program;
            return 1;
        }

        NES::Logger::setupLogging("nes-cli.log", NES::LogLevel::LOG_WARNING, program.is_used("-d"));
        if (program.get<bool>("-d"))
        {
            NES::Logger::getInstance()->changeLogLevel(NES::LogLevel::LOG_DEBUG);
        }

        if (auto subcommand = std::ranges::find_if(
                queryManagementSubcommands, [&](auto& subparser) { return program.is_subcommand_used(subparser.get()); });
            subcommand != queryManagementSubcommands.end())
        {
            doQueryManagement(program, *subcommand);
            return 0;
        }

        if (auto subcommand
            = std::ranges::find_if(querySubmissionCommands, [&](auto& subparser) { return program.is_subcommand_used(subparser.get()); });
            subcommand != querySubmissionCommands.end())
        {
            doQuerySubmission(program, *subcommand);
            return 0;
        }

        std::cerr << "No subcommand used.\n";
        std::cerr << program;
        return 1;
    }

    CPPTRACE_CATCH(...)
    {
        NES::tryLogCurrentException();
        return 1;
    }
}
