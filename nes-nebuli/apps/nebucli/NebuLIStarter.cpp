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
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <ranges>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
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
#include <Util/Files.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/Pointers.hpp>
#include <argparse/argparse.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <nlohmann/json.hpp> ///NOLINT(misc-include-cleaner)
#include <nlohmann/json_fwd.hpp>
#include <yaml-cpp/exceptions.h>
#include <yaml-cpp/node/convert.h>
#include <yaml-cpp/node/node.h>
#include <yaml-cpp/node/parse.h>
#include <yaml-cpp/yaml.h> ///NOLINT(misc-include-cleaner)
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <LegacyOptimizer.hpp>
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

std::string bindIdentifierName(std::string_view identifier)
{
    auto verifyAllowedCharacters = [](std::string_view potentiallyInvalid)
    {
        if (!std::ranges::all_of(potentiallyInvalid, [](char c) { return std::isalnum(c) || c == '_' || c == '$'; }))
        {
            throw NES::InvalidIdentifier("{}", potentiallyInvalid);
        }
    };

    if (identifier.size() > 2 && identifier.starts_with('`') && identifier.ends_with('`'))
    {
        /// remove backticks and keep name as is;
        verifyAllowedCharacters(identifier.substr(1, identifier.size() - 2));
        return std::string(identifier.substr(1, identifier.size() - 2));
    }

    verifyAllowedCharacters(identifier);
    return NES::toUpperCase(identifier);
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
    std::string grpc;
    size_t capacity{};
    std::vector<std::string> downstream;
};

struct QueryConfig
{
    std::vector<std::string> query;
    std::vector<Sink> sinks;
    std::vector<LogicalSource> logical;
    std::vector<PhysicalSource> physical;
    std::vector<WorkerConfig> workers;
};
}

namespace YAML
{
template <>
struct convert<NES::CLI::SchemaField>
{
    static bool decode(const Node& node, NES::CLI::SchemaField& rhs)
    {
        rhs.name = bindIdentifierName(node["name"].as<std::string>());
        rhs.type = stringToFieldType(node["type"].as<std::string>());
        return true;
    }
};

template <>
struct convert<NES::CLI::Sink>
{
    static bool decode(const Node& node, NES::CLI::Sink& rhs)
    {
        rhs.name = bindIdentifierName(node["name"].as<std::string>());
        rhs.type = node["type"].as<std::string>();
        rhs.schema = node["schema"].as<std::vector<NES::CLI::SchemaField>>();
        rhs.host = node["host"].as<std::string>();
        rhs.config = node["config"].as<std::unordered_map<std::string, std::string>>();
        return true;
    }
};

template <>
struct convert<NES::CLI::LogicalSource>
{
    static bool decode(const Node& node, NES::CLI::LogicalSource& rhs)
    {
        rhs.name = bindIdentifierName(node["name"].as<std::string>());
        rhs.schema = node["schema"].as<std::vector<NES::CLI::SchemaField>>();
        return true;
    }
};

template <>
struct convert<NES::CLI::PhysicalSource>
{
    static bool decode(const Node& node, NES::CLI::PhysicalSource& rhs)
    {
        rhs.logical = bindIdentifierName(node["logical"].as<std::string>());
        rhs.type = node["type"].as<std::string>();
        rhs.host = node["host"].as<std::string>();
        rhs.parserConfig = node["parser_config"].as<std::unordered_map<std::string, std::string>>();
        rhs.sourceConfig = node["source_config"].as<std::unordered_map<std::string, std::string>>();
        return true;
    }
};

template <>
struct convert<NES::CLI::WorkerConfig>
{
    static bool decode(const Node& node, NES::CLI::WorkerConfig& rhs)
    {
        rhs.capacity = node["capacity"].as<size_t>();
        if (node["downstream"].IsDefined())
        {
            rhs.downstream = node["downstream"].as<std::vector<std::string>>();
        }
        rhs.grpc = node["grpc"].as<std::string>();
        rhs.host = node["host"].as<std::string>();

        return true;
    }
};

template <>
struct convert<NES::CLI::QueryConfig>
{
    static bool decode(const Node& node, NES::CLI::QueryConfig& rhs)
    {
        rhs.sinks = node["sinks"].as<std::vector<NES::CLI::Sink>>();
        rhs.logical = node["logical"].as<std::vector<NES::CLI::LogicalSource>>();
        rhs.physical = node["physical"].as<std::vector<NES::CLI::PhysicalSource>>();
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
        rhs.workers = node["workers"].as<std::vector<NES::CLI::WorkerConfig>>();
        return true;
    }
};
}

namespace NES
{
struct PersistentQueryId
{
    DistributedQuery query;

    std::string store(const DistributedQueryId& id)
    {
        const std::filesystem::path path(id.getRawValue());
        std::ofstream output(path);
        const nlohmann::json json(query.getLocalQueries());
        output << json.dump(4);
        return id.getRawValue();
    }

    static PersistentQueryId load(std::string_view persistentId)
    {
        std::filesystem::path path(persistentId);
        if (!exists(path))
        {
            throw InvalidConfigParameter(fmt::format("Could not find query with id {}", persistentId));
        }
        std::ifstream file(path);
        if (!file)
        {
            throw InvalidConfigParameter(fmt::format("Could not open file: {}", path));
        }
        PersistentQueryId result(DistributedQuery(
            nlohmann::json::parse(file).get<std::remove_cvref_t<decltype(std::declval<DistributedQuery>().getLocalQueries())>>()));
        return result;
    }
};
}

namespace
{
NES::CLI::QueryConfig getTopologyPath(const argparse::ArgumentParser& parser)
{
    std::vector<std::string> options;
    if (parser.is_used("-t"))
    {
        options.push_back(parser.get<std::string>("-t"));
    }
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
            NES_WARNING("{} is not a valid yaml file: {} ({}:{})", option, e.what(), e.mark.line, e.mark.column, e.what());
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

std::vector<NES::Statement> loadStatements(const NES::CLI::QueryConfig& topologyConfig)
{
    const auto& [query, sinks, logical, physical, workers] = topologyConfig;
    std::vector<NES::Statement> statements;
    statements.reserve(workers.size());
    for (const auto& [host, grpc, capacity, downstream] : workers)
    {
        statements.emplace_back(NES::CreateWorkerStatement{.host = host, .grpc = grpc, .capacity = capacity, .downstream = downstream});
    }
    for (const auto& [name, schemaFields] : logical)
    {
        NES::Schema schema;
        for (const auto& schemaField : schemaFields)
        {
            schema.addField(schemaField.name, schemaField.type);
        }

        statements.emplace_back(NES::CreateLogicalSourceStatement{.name = name, .schema = schema});
    }

    for (const auto& [logical, type, host, parserConfig, sourceConfig] : physical)
    {
        auto sourceConfigCopy = sourceConfig;
        sourceConfigCopy.emplace("host", host);
        statements.emplace_back(NES::CreatePhysicalSourceStatement{
            .attachedTo = NES::LogicalSourceName(logical),
            .sourceType = type,
            .sourceConfig = sourceConfigCopy,
            .parserConfig = parserConfig});
    }
    for (const auto& [name, schemaFields, type, host, config] : sinks)
    {
        NES::Schema schema;
        for (const auto& schemaField : schemaFields)
        {
            schema.addField(schemaField.name, schemaField.type);
        }

        auto configCopy = config;
        configCopy.emplace("host", host);
        statements.emplace_back(NES::CreateSinkStatement{.name = name, .sinkType = type, .schema = schema, .sinkConfig = configCopy});
    }
    return statements;
}

void doStatus(
    NES::QueryStatementHandler& queryStatementHandler,
    NES::TopologyStatementHandler& topologyStatementHandler,
    const std::vector<NES::DistributedQueryId>& queries)
{
    if (queries.empty())
    {
        auto result = topologyStatementHandler(NES::WorkerStatusStatement{{}});
        std::cout << nlohmann::json(NES::StatementOutputAssembler<NES::WorkerStatusStatementResult>{}.convert(result.value())).dump(4)
                  << '\n';
    }
    else
    {
        auto result = nlohmann::json::array();
        for (const auto& query : queries)
        {
            auto statementResult
                = queryStatementHandler(NES::ShowQueriesStatement{.id = query, .format = NES::StatementOutputFormat::JSON});
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

    const auto state
        = subcommand.get<std::vector<std::string>>("queryId")
        | std::views::transform(
              [](const std::string& queryId) -> std::pair<NES::DistributedQueryId, NES::DistributedQuery>
              { return {NES::DistributedQueryId(queryId), NES::DistributedQuery{NES::PersistentQueryId::load(queryId).query}}; })
        | std::ranges::to<std::unordered_map>();

    const auto queries = state | std::views::keys | std::ranges::to<std::vector>();

    auto statements = loadStatements(topologyConfig);
    auto workerCatalog = std::make_shared<NES::WorkerCatalog>();
    auto sourceCatalog = std::make_shared<NES::SourceCatalog>();
    auto sinkCatalog = std::make_shared<NES::SinkCatalog>();
    const auto queryManager = std::make_shared<NES::QueryManager>(workerCatalog, NES::createGRPCBackend(), NES::QueryManagerState{state});

    NES::TopologyStatementHandler topologyHandler{queryManager, workerCatalog};
    NES::SourceStatementHandler sourceHandler{sourceCatalog, NES::RequireHostConfig{}};
    NES::SinkStatementHandler sinkHandler{sinkCatalog, NES::RequireHostConfig{}};
    auto optimizer = std::make_shared<NES::LegacyOptimizer>(sourceCatalog, sinkCatalog, workerCatalog);
    NES::QueryStatementHandler queryHandler{queryManager, optimizer};

    handleStatements(loadStatements(topologyConfig), topologyHandler, sourceHandler, sinkHandler);

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
    if (queries.empty())
    {
        throw NES::InvalidConfigParameter("No queries");
    }

    auto workerCatalog = std::make_shared<NES::WorkerCatalog>();
    auto sourceCatalog = std::make_shared<NES::SourceCatalog>();
    auto sinkCatalog = std::make_shared<NES::SinkCatalog>();
    auto queryManager = std::make_shared<NES::QueryManager>(workerCatalog, NES::createGRPCBackend());

    NES::TopologyStatementHandler topologyHandler{queryManager, workerCatalog};
    NES::SourceStatementHandler sourceHandler{sourceCatalog, NES::RequireHostConfig{}};
    NES::SinkStatementHandler sinkHandler{sinkCatalog, NES::RequireHostConfig{}};
    auto optimizer = std::make_shared<NES::LegacyOptimizer>(sourceCatalog, sinkCatalog, workerCatalog);
    handleStatements(statements, topologyHandler, sourceHandler, sinkHandler);

    if (program.is_subcommand_used("start"))
    {
        NES::QueryStatementHandler queryStatementHandler{queryManager, optimizer};
        for (const auto& query : queries)
        {
            auto result = queryStatementHandler(NES::QueryStatement(NES::AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query)));
            if (result)
            {
                auto queryDescriptor = queryManager->getQuery(result->id);
                INVARIANT(queryDescriptor.has_value(), "Query should exist in the query manager if statement handler succeed");
                NES::PersistentQueryId persistentId(*queryDescriptor);
                std::cout << persistentId.store(result->id) << '\n';
            }
            else
            {
                throw std::move(result.error());
            }
        }
    }
    else
    {
        NES::QueryStatementHandler queryStatementHandler{queryManager, optimizer};
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
        using argparse::ArgumentParser;
        ArgumentParser program("nebucli");
        program.add_argument("-d", "--debug").flag().help("Dump the query plan and enable debug logging");
        program.add_argument("-t").help(
            "Path to the topology file. If this flag is not used it will fallback to the NES_TOPOLOGY_FILE environment");

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

        NES::Logger::setupLogging("nebucli.log", NES::LogLevel::LOG_WARNING, program.is_used("-d"));
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
