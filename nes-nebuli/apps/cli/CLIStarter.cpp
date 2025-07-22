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
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <ranges>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <unistd.h>
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
#include <Util/Signal.hpp>
#include <Util/Strings.hpp>
#include <argparse/argparse.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <nlohmann/json.hpp> ///NOLINT(misc-include-cleaner)
#include <yaml-cpp/yaml.h> ///NOLINT(misc-include-cleaner)
#include <ErrorHandling.hpp>
#include <LegacyOptimizer.hpp>
#include <QueryStateBackend.hpp>

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
        if (!std::ranges::all_of(
                potentiallyInvalid, [](char character) { return std::isalnum(character) || character == '_' || character == '$'; }))
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
    std::unordered_map<std::string, std::string> parserConfig;
    std::unordered_map<std::string, std::string> sourceConfig;
};

struct QueryConfig
{
    std::vector<std::string> query;
    std::vector<Sink> sinks;
    std::vector<LogicalSource> logical;
    std::vector<PhysicalSource> physical;
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
        rhs.parserConfig = node["parser_config"].as<std::unordered_map<std::string, std::string>>();
        rhs.sourceConfig = node["source_config"].as<std::unordered_map<std::string, std::string>>();
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
            if (isatty(STDIN_FILENO))
            {
                throw NES::InvalidConfigParameter("Cannot read topology from stdin: stdin is a terminal");
            }
            try
            {
                std::stringstream buffer;
                buffer << std::cin.rdbuf();
                std::string yamlContent = buffer.str();
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
    const auto& [query, sinks, logical, physical] = topologyConfig;
    std::vector<NES::Statement> statements;
    for (const auto& [name, schemaFields] : logical)
    {
        NES::Schema schema;
        for (const auto& schemaField : schemaFields)
        {
            schema.addField(schemaField.name, schemaField.type);
        }

        statements.emplace_back(NES::CreateLogicalSourceStatement{.name = name, .schema = schema});
    }

    for (const auto& [logical, type, parserConfig, sourceConfig] : physical)
    {
        auto sourceConfigCopy = sourceConfig;
        sourceConfigCopy.emplace("host", "localhost:9090");
        statements.emplace_back(NES::CreatePhysicalSourceStatement{
            .attachedTo = NES::LogicalSourceName(logical),
            .sourceType = type,
            .sourceConfig = sourceConfigCopy,
            .parserConfig = parserConfig});
    }
    for (const auto& [name, schemaFields, type, config] : sinks)
    {
        NES::Schema schema;
        for (const auto& schemaField : schemaFields)
        {
            schema.addField(schemaField.name, schemaField.type);
        }

        auto configCopy = config;
        configCopy.emplace("host", "localhost:9090");
        statements.emplace_back(NES::CreateSinkStatement{.name = name, .sinkType = type, .schema = schema, .sinkConfig = configCopy});
    }
    return statements;
}

void doStatus(
    NES::QueryStatementHandler& queryStatementHandler,
    NES::TopologyStatementHandler& topologyStatementHandler,
    const std::unordered_map<NES::QueryId, std::string>& queries)
{
    if (queries.empty())
    {
        auto result = topologyStatementHandler(NES::WorkerStatusStatement{});
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
        for (const auto& query : queries | std::views::keys)
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

        /// Patch local query ids for persistent id
        for (auto& query : result)
        {
            auto localIdStr = query["local_query_id"].get<std::string>();
            auto queryId = NES::QueryId(NES::LocalQueryId(localIdStr));
            query["query_id"] = queries.at(queryId);
            query.erase("local_query_id");
        }

        std::cout << result.dump(4) << '\n';
    }
}

void doStop(NES::QueryStatementHandler& queryStatementHandler, const std::unordered_map<NES::QueryId, std::string>& queries)
{
    auto result = nlohmann::json::array();
    for (const auto& query : queries | std::views::keys)
    {
        auto statementResult = queryStatementHandler(NES::DropQueryStatement{.id = query});
        if (!statementResult)
        {
            throw std::move(statementResult.error());
        }

        nlohmann::json results(NES::StatementOutputAssembler<NES::DropQueryStatementResult>{}.convert(statementResult.value()));
        result.insert(result.end(), results.begin(), results.end());
    }

    /// Patch local query ids for persistent id
    for (auto& query : result)
    {
        auto localIdStr = query["local_query_id"].get<std::string>();
        auto queryId = NES::QueryId(NES::LocalQueryId(localIdStr));
        query["query_id"] = queries.at(queryId);
        query.erase("local_query_id");
    }

    std::cout << result.dump(4) << '\n';
}

NES::HostAddr hostAddr{"localhost:9090"};
NES::GrpcAddr grpcAddr{"localhost:8080"};

NES::UniquePtr<NES::GRPCQuerySubmissionBackend> createGRPCBackend(const argparse::ArgumentParser& program)
{
    if (program.is_used("-s"))
    {
        return std::make_unique<NES::GRPCQuerySubmissionBackend>(
            NES::WorkerConfig{.host = hostAddr, .grpc = NES::GrpcAddr(program.get("-s")), .config = {}});
    }
    if (auto* env = std::getenv("NES_WORKER_GRPC_ADDR"))
    {
        NES_DEBUG("Found worker address in environment: {}", env);
        return std::make_unique<NES::GRPCQuerySubmissionBackend>(
            NES::WorkerConfig{.host = hostAddr, .grpc = NES::GrpcAddr(env), .config = {}});
    }

    return std::make_unique<NES::GRPCQuerySubmissionBackend>(NES::WorkerConfig{.host = hostAddr, .grpc = grpcAddr, .config = {}});
}

void doQueryManagement(const argparse::ArgumentParser& program, const argparse::ArgumentParser& subcommand)
{
    const auto topologyConfig = getTopologyPath(program);
    NES::CLI::QueryStateBackend stateBackend;

    const auto mapping = subcommand.get<std::vector<std::string>>("queryId")
        | std::views::transform(
                             [&stateBackend](const auto& persistentIdString) -> std::pair<NES::QueryId, std::string>
                             {
                                 auto persistedId = NES::CLI::PersistedQueryId::fromString(persistentIdString);
                                 auto queryId = stateBackend.load(persistedId);
                                 return {queryId, persistentIdString};
                             })
        | std::ranges::to<std::unordered_map>();
    const auto state = mapping | std::views::keys | std::ranges::to<std::unordered_set>();

    auto statements = loadStatements(topologyConfig);
    auto sourceCatalog = std::make_shared<NES::SourceCatalog>();
    auto sinkCatalog = std::make_shared<NES::SinkCatalog>();
    const auto queryManager = std::make_shared<NES::QueryManager>(createGRPCBackend(program), NES::QueryManagerState{state});

    NES::TopologyStatementHandler topologyHandler{queryManager};
    NES::SourceStatementHandler sourceHandler{sourceCatalog, NES::RequireHostConfig{}};
    NES::SinkStatementHandler sinkHandler{sinkCatalog, NES::RequireHostConfig{}};
    auto optimizer = std::make_shared<NES::LegacyOptimizer>(sourceCatalog, sinkCatalog);
    NES::QueryStatementHandler queryHandler{queryManager, optimizer};

    handleStatements(loadStatements(topologyConfig), topologyHandler, sourceHandler, sinkHandler);

    if (program.is_subcommand_used("stop"))
    {
        doStop(queryHandler, mapping);
    }
    else if (program.is_subcommand_used("status"))
    {
        doStatus(queryHandler, topologyHandler, mapping);
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

    auto sourceCatalog = std::make_shared<NES::SourceCatalog>();
    auto sinkCatalog = std::make_shared<NES::SinkCatalog>();
    auto queryManager = std::make_shared<NES::QueryManager>(createGRPCBackend(program));

    NES::TopologyStatementHandler topologyHandler{queryManager};
    NES::SourceStatementHandler sourceHandler{sourceCatalog, NES::RequireHostConfig{}};
    NES::SinkStatementHandler sinkHandler{sinkCatalog, NES::RequireHostConfig{}};
    auto optimizer = std::make_shared<NES::LegacyOptimizer>(sourceCatalog, sinkCatalog);
    handleStatements(statements, topologyHandler, sourceHandler, sinkHandler);

    if (program.is_subcommand_used("start"))
    {
        NES::CLI::QueryStateBackend stateBackend;
        NES::QueryStatementHandler queryStatementHandler{queryManager, optimizer};
        for (const auto& query : queries)
        {
            auto result = queryStatementHandler(NES::QueryStatement(NES::AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query)));
            if (result)
            {
                auto queryDescriptor = queryManager->getQuery(result->id);
                INVARIANT(queryDescriptor.has_value(), "Query should exist in the query manager if statement handler succeed");
                auto persistedId = stateBackend.store(*queryDescriptor);
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
        NES::setupSignalHandlers();
        using argparse::ArgumentParser;
        ArgumentParser program("nebucli");
        program.add_argument("-d", "--debug").flag().help("Dump the query plan and enable debug logging");
        program.add_argument("-s", "--server").help("Worker gRPC endpoint URL (default: localhost:8080 or NES_WORKER_GRPC_ADDR if set)");
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
