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
#include <memory>
#include <optional>
#include <ranges>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>
#include <unistd.h>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/SchemaBase.hpp>
#include <DataTypes/SchemaBaseFwd.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongTypeJson.hpp> ///NOLINT(misc-include-cleaner)
#include <QueryManager/GRPCQuerySubmissionBackend.hpp>
#include <QueryManager/QueryManager.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Statements/JsonOutputFormatter.hpp> ///NOLINT(misc-include-cleaner)
#include <Statements/StatementHandler.hpp>
#include <Statements/StatementOutputAssembler.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/Pointers.hpp>
#include <Util/Signal.hpp>
#include <Util/Strings.hpp>
#include <argparse/argparse.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/ranges.h>
#include <nlohmann/json.hpp> ///NOLINT(misc-include-cleaner)
#include <yaml-cpp/node/node.h>
#include <yaml-cpp/yaml.h> ///NOLINT(misc-include-cleaner)
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <ModelCatalog.hpp>
#include <QueryOptimizer.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <QueryStateBackend.hpp>
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

struct QueryConfig
{
    std::vector<std::string> query;
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
        acceptKeys({"name", "type"}, node);
        rhs.name = getValue<std::string>(node, "name");
        rhs.type = stringToFieldType(getValue<std::string>(node, "type"));
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
                rhs.query = node["query"].as<std::vector<std::string>>();
            }
            else
            {
                rhs.query.emplace_back(node["query"].as<std::string>());
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

std::vector<std::string> loadQueries(
    const argparse::ArgumentParser& /*parser*/, const argparse::ArgumentParser& subcommand, const NES::CLI::QueryConfig& topologyConfig)
{
    std::vector<std::string> queries;
    if (subcommand.is_used("queries"))
    {
        for (const auto& query : subcommand.get<std::vector<std::string>>("queries"))
        {
            queries.emplace_back(query);
        }
        NES_DEBUG("loaded {} queries from commandline", queries.size());
    }
    else
    {
        for (const auto& query : topologyConfig.query)
        {
            queries.emplace_back(query);
        }
        NES_DEBUG("loaded {} queries from topology file", queries.size());
    }
    return queries;
}

std::unordered_map<NES::Identifier, std::string> bindConfig(const std::unordered_map<std::string, std::string>& config)
{
    return config
        | std::views::transform([](const auto& rawPair) { return std::make_pair(bindIdentifierName(rawPair.first), rawPair.second); })
        | std::ranges::to<std::unordered_map<NES::Identifier, std::string>>();
}

NES::Schema<NES::UnqualifiedUnboundField, NES::Ordered> bindSchema(const std::vector<NES::CLI::SchemaField>& schemaFields)
{
    return schemaFields
        | std::views::transform([](const auto& rawField)
                                { return NES::UnqualifiedUnboundField{bindIdentifierName(rawField.name), rawField.type}; })
        | std::ranges::to<NES::Schema<NES::UnqualifiedUnboundField, NES::Ordered>>();
}

std::vector<NES::Statement> loadStatements(const NES::CLI::QueryConfig& topologyConfig)
{
    const auto& [query, sinks, logical, physical, optimizer, workers, models] = topologyConfig;
    std::vector<NES::Statement> statements;
    statements.reserve(workers.size());
    for (const auto& [host, dataAddress, maxOperators, downstream, config] : workers)
    {
        statements.emplace_back(NES::CreateWorkerStatement{
            .host = host, .dataAddress = dataAddress, .capacity = maxOperators, .downstream = downstream, .config = config});
    }
    for (const auto& [name, schemaFields] : logical)
    {
        statements.emplace_back(NES::CreateLogicalSourceStatement{.name = bindIdentifierName(name), .schema = bindSchema(schemaFields)});
    }

    for (const auto& [logical, type, host, parserConfig, sourceConfig] : physical)
    {
        statements.emplace_back(NES::CreatePhysicalSourceStatement{
            .attachedTo = bindIdentifierName(logical),
            .sourceType = bindIdentifierName(type),
            .host = NES::Host(host),
            .sourceConfig = bindConfig(sourceConfig),
            .parserConfig = bindConfig(parserConfig)});
    }
    for (const auto& [name, schemaFields, type, host, config, parserConfig] : sinks)
    {
        statements.emplace_back(NES::CreateSinkStatement{
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
                | std::views::transform([](const NES::CLI::SchemaField& field)
                                        { return NES::UnqualifiedUnboundField{NES::Identifier::parse(field.name), field.type}; })
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
        auto jsonResult = nlohmann::json(NES::StatementOutputAssembler<NES::WorkerStatusStatementResult>{}.convert(result.value()));
        std::cout << jsonResult.dump(4) << '\n';
    }
    else
    {
        auto result = nlohmann::json::array();
        for (const auto& query : queries)
        {
            auto statementResult
                = queryStatementHandler(NES::ShowQueriesStatement{.id = query, .format = NES::StatementOutputFormat::JSON});
            if (!statementResult)
            {
                throw std::move(statementResult.error());
            }

            nlohmann::json results(NES::StatementOutputAssembler<NES::ShowQueriesStatementResult>{}.convert(statementResult.value()));
            result.insert(result.end(), results.begin(), results.end());
        }

        std::cout << result.dump(4) << '\n';
    }
}

void doStop(NES::QueryStatementHandler& queryStatementHandler, const std::vector<NES::DistributedQueryId>& queries)
{
    auto result = nlohmann::json::array();
    for (const auto& query : queries)
    {
        auto statementResult = queryStatementHandler(NES::DropQueryStatement{.id = query});
        if (!statementResult)
        {
            throw std::move(statementResult.error());
        }

        nlohmann::json results(NES::StatementOutputAssembler<NES::DropQueryStatementResult>{}.convert(statementResult.value()));
        result.insert(result.end(), results.begin(), results.end());
    }

    std::cout << result.dump(4) << '\n';
}

void doQueryManagement(const argparse::ArgumentParser& program, const argparse::ArgumentParser& subcommand)
{
    const auto topologyConfig = getTopologyPath(program);
    auto queryOptimizationConfiguration = loadQueryOptimizerConfiguration(topologyConfig);
    NES::CLI::QueryStateBackend stateBackend;

    const auto state = subcommand.get<std::vector<std::string>>("queryId")
        | std::views::transform(
                           [&stateBackend](const std::string& queryId) -> std::pair<NES::DistributedQueryId, NES::DistributedQuery>
                           {
                               auto persistedId = NES::CLI::PersistedQueryId::fromString(queryId);
                               auto distributedQuery = stateBackend.load(persistedId);
                               return {persistedId.queryId, distributedQuery};
                           })
        | std::ranges::to<std::unordered_map>();

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

    if (program.is_subcommand_used("start"))
    {
        NES::CLI::QueryStateBackend stateBackend;
        NES::QueryStatementHandler queryStatementHandler{queryManager, queryOptimizer};
        for (const auto& query : queries)
        {
            auto result = queryStatementHandler(NES::QueryStatement(NES::AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query)));
            if (result)
            {
                auto queryDescriptor = queryManager->getQuery(result->id);
                INVARIANT(queryDescriptor.has_value(), "Query should exist in the query manager if statement handler succeed");
                auto persistedId = stateBackend.store(result->id, *queryDescriptor);
                std::cout << persistedId.toString() << '\n';
            }
            else
            {
                throw std::move(result.error());
            }
        }
    }
    else
    {
        NES::QueryStatementHandler queryStatementHandler{queryManager, queryOptimizer};
        for (const auto& query : queries)
        {
            auto result
                = queryStatementHandler(NES::ExplainQueryStatement(NES::AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query)));
            if (result)
            {
                std::cout << result->explainString << "\n";
            }
            else
            {
                throw std::move(result.error());
            }
        }
    }
}
}

int main(int argc, char** argv)
{
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
        stopQuery.add_argument("queryId").nargs(argparse::nargs_pattern::at_least_one);

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
