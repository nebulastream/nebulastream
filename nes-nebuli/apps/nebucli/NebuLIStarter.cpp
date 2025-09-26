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
#include <cerrno>
#include <chrono>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <ostream>
#include <thread>
#include <utility>
#include <vector>
#include <unistd.h>

#include <DataTypes/DataTypeProvider.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryManager/GRPCQuerySubmissionBackend.hpp>
#include <QueryManager/QueryManager.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Common.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <argparse/argparse.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <google/protobuf/text_format.h>
#include <grpcpp/security/credentials.h>
#include <yaml-cpp/yaml.h>
#include <ErrorHandling.hpp>
#include <SingleNodeWorkerRPCService.grpc.pb.h>
#include <StatementHandler.hpp>
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
    size_t capacity;
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
        rhs.name = node["name"].as<std::string>();
        rhs.type = stringToFieldType(node["type"].as<std::string>());
        return true;
    }
};

template <>
struct convert<NES::CLI::Sink>
{
    static bool decode(const Node& node, NES::CLI::Sink& rhs)
    {
        rhs.name = node["name"].as<std::string>();
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
        rhs.name = node["name"].as<std::string>();
        rhs.schema = node["schema"].as<std::vector<NES::CLI::SchemaField>>();
        return true;
    }
};

template <>
struct convert<NES::CLI::PhysicalSource>
{
    static bool decode(const Node& node, NES::CLI::PhysicalSource& rhs)
    {
        rhs.logical = node["logical"].as<std::string>();
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

static NES::CLI::QueryConfig getTopologyPath(const argparse::ArgumentParser& parser)
{
    std::vector<std::string> options;
    if (parser.is_used("-t"))
    {
        options.push_back(parser.get<std::string>("-t"));
    }
    if (const auto env = std::getenv("NES_TOPOLOGY_FILE"))
    {
        options.push_back(env);
    }
    options.push_back("topology.yaml");
    options.push_back("topology.yml");

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
        catch (std::exception& e)
        {
            NES_WARNING("{} is not a valid yaml file: {}", option, e.what());
            /// That wasn't a valid yaml file
        }
    }
    throw NES::InvalidConfigParameter("Could not find topology file");
}

std::vector<std::string>
loadQueries(const argparse::ArgumentParser& parser, const argparse::ArgumentParser& subcommand, const NES::CLI::QueryConfig& topologyConfig)
{
    std::vector<std::string> queries;
    if (subcommand.is_used("queries"))
    {
        for (auto query : subcommand.get<std::vector<std::string>>("queries"))
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
        statements.emplace_back(NES::CreatePhysicalSourceStatement{
            .attachedTo = NES::LogicalSourceName(logical),
            .sourceType = type,
            .workerId = host,
            .sourceConfig = sourceConfig,
            .parserConfig = NES::ParserConfig::create(parserConfig)});
    }
    for (const auto& [name, schemaFields, type, host, config] : sinks)
    {
        NES::Schema schema;
        for (const auto& schemaField : schemaFields)
        {
            schema.addField(schemaField.name, schemaField.type);
        }

        statements.emplace_back(
            NES::CreateSinkStatement{.name = name, .sinkType = type, .workerId = host, .schema = schema, .sinkConfig = config});
    }
    return statements;
}

void doQueryManagement(const argparse::ArgumentParser&, const argparse::ArgumentParser&)
{
}

template <typename HandlerT>
bool tryCall(const NES::Statement& statement, HandlerT& handler)
{
    return std::visit(
        [&]<typename StatementType>(const StatementType& typedStatement)
        {
            if constexpr (std::is_invocable_v<HandlerT&, const StatementType&>)
            {
                if (auto value = handler(typedStatement); !value)
                {
                    throw value.error();
                }
                return true;
            }
            return false;
        },
        statement);
}

template <typename HandlerT, typename... HandlerTs>
bool tryCall(const NES::Statement& statement, HandlerT& handler, HandlerTs&... handlers)
{
    auto couldHandle = std::visit(
        [&]<typename StatementType>(const StatementType& typedStatement)
        {
            if constexpr (std::is_invocable_v<HandlerT&, const StatementType&>)
            {
                if (auto value = handler(typedStatement); !value)
                {
                    throw value.error();
                }
                return true;
            }
            return false;
        },
        statement);
    if (couldHandle)
    {
        return true;
    }
    return tryCall(statement, handlers...);
}

template <typename... HandlerT>
void handleStatements(std::vector<NES::Statement> statements, HandlerT&... handler)
{
    for (const auto& statement : statements)
    {
        tryCall(statement, handler...);
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

    NES::TopologyStatementHandler topologyHandler{workerCatalog};
    NES::SourceStatementHandler sourceHandler{sourceCatalog};
    NES::SinkStatementHandler sinkHandler{sinkCatalog};
    handleStatements(statements, topologyHandler, sourceHandler, sinkHandler);

    if (program.is_subcommand_used("start"))
    {
        auto queryManager
            = std::make_shared<NES::QueryManager>(std::make_unique<NES::GRPCQuerySubmissionBackend>(workerCatalog->getAllWorkers()));
        NES::QueryStatementHandler queryStatementHandler{queryManager, sourceCatalog, sinkCatalog, workerCatalog};
        for (const auto& query : queries)
        {
            queryStatementHandler(NES::QueryStatement(NES::AntlrSQLQueryParser::createLogicalQueryPlanFromSQLString(query)));
        }
    }
    else
    {
        auto queryManager
            = std::make_shared<NES::QueryManager>(std::make_unique<NES::GRPCQuerySubmissionBackend>(std::vector<NES::WorkerConfig>{}));
        NES::QueryStatementHandler queryStatementHandler{queryManager, sourceCatalog, sinkCatalog, workerCatalog};
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
                throw result.error();
            }
        }
    }
}

int main(int argc, char** argv)
{
    CPPTRACE_TRY
    {
        NES::Logger::setupLogging("nebucli.log", NES::LogLevel::LOG_WARNING, true);

        using argparse::ArgumentParser;
        ArgumentParser program("nebucli");
        program.add_argument("-d", "--debug").flag().help("Dump the query plan and enable debug logging");
        program.add_argument("-t").help(
            "Path to the topology file. If this flag is not used it will fallback to the NES_TOPOLOGY_FILE environment");

        ArgumentParser startQuery("start");
        startQuery.add_argument("queries").nargs(argparse::nargs_pattern::any);

        ArgumentParser stopQuery("stop");
        stopQuery.add_argument("queryId").scan<'i', size_t>();

        ArgumentParser dump("dump");
        dump.add_argument("queries").nargs(argparse::nargs_pattern::any);

        program.add_subparser(startQuery);
        program.add_subparser(stopQuery);
        program.add_subparser(dump);

        std::vector<std::reference_wrapper<ArgumentParser>> queryManagementSubcommands{stopQuery};
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
        return NES::getCurrentErrorCode();
    }
}
