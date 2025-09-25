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
#include <climits>
#include <cstddef>
#include <cstring>
#include <format>
#include <fstream>
#include <functional>
#include <initializer_list>
#include <iostream>
#include <memory>
#include <optional>
#include <ostream>
#include <thread>
#include <utility>
#include <vector>
#include <unistd.h>

#include <GlobalOptimizer/GlobalOptimizer.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryManager/GRPCQuerySubmissionBackend.hpp>
#include <QueryManager/QueryManager.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <argparse/argparse.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <google/protobuf/text_format.h>
#include <grpcpp/security/credentials.h>
#include <yaml-cpp/node/parse.h>
#include "DataTypes/DataTypeProvider.hpp"
#include "Util/Common.hpp"

#include <ErrorHandling.hpp>
#include <SingleNodeWorkerRPCService.grpc.pb.h>
#include "WorkerCatalog.hpp"

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
    std::string query;
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
        rhs.query = node["query"].as<std::string>();
        rhs.workers = node["workers"].as<std::vector<NES::CLI::WorkerConfig>>();
        return true;
    }
};
}

static NES::CLI::QueryConfig getTopologyPath(argparse::ArgumentParser& parser)
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

std::vector<NES::Statement> loadStatements(argparse::ArgumentParser& parser)
{
    auto config = getTopologyPath(parser);
    std::vector<NES::Statement> statements;
    for (const auto& worker : config.workers)
    {
        statements.push_back(NES::CreateWorkerStatement{
            .host = worker.host, .grpc = worker.grpc, .capacity = worker.capacity, .downstream = worker.downstream});
    }
    for (const auto& logical : config.logical)
    {
        NES::Schema schema;
        for (const auto& schemaField : logical.schema)
        {
            schema.addField(schemaField.name, schemaField.type);
        }

        statements.push_back(NES::CreateLogicalSourceStatement{.name = logical.name, .schema = schema});
    }

    for (const auto& physical : config.physical)
    {
        statements.push_back(NES::CreatePhysicalSourceStatement{
            .attachedTo = physical.logical,
            .sourceType = physical.type,
            .workerId = NES::WorkerId(physical.host),
            .sourceConfig = physical.sourceConfig,
            .parserConfig = physical.parserConfig
        });
    }
}

struct State
{
    NES::SharedPtr<NES::SourceCatalog> sourceCatalog;
    NES::SharedPtr<NES::SinkCatalog> sinkCatalog;
    NES::SharedPtr<NES::WorkerCatalog> workerCatalog;
    std::string queryString;

    static State load(argparse::ArgumentParser& parser)
    {
        State state{
            std::make_shared<NES::SourceCatalog>(), std::make_shared<NES::SinkCatalog>(), std::make_shared<NES::WorkerCatalog>(), ""};
        NES::CLI::YAMLBinder binder{
            NES::Util::copyPtr(state.sourceCatalog), NES::Util::copyPtr(state.sinkCatalog), NES::Util::copyPtr(state.workerCatalog)};

        std::ifstream file{getTopologyPath(parser)};
        binder.parseAndBind(file);
        if (parser.is_used("query"))
        {
        }
    }
};

int main(int argc, char** argv)
{
    CPPTRACE_TRY
    {
        NES::Logger::setupLogging("nebucli.log", NES::LogLevel::LOG_ERROR, true);

        using argparse::ArgumentParser;
        ArgumentParser program("nebucli");
        program.add_argument("-d", "--debug").flag().help("Dump the query plan and enable debug logging");
        program.add_argument("-t").help(
            "Path to the topology file. If this flag is not used it will fallback to the NES_TOPOLOGY_FILE environment");

        ArgumentParser registerQuery("register");
        registerQuery.add_argument("-i", "--input")
            .default_value("-")
            .help("Read the query description. Use - for stdin which is the default");
        registerQuery.add_argument("-x").help("Immediately start the query as well").flag();

        ArgumentParser startQuery("start");
        startQuery.add_argument("queryId").scan<'i', size_t>();

        ArgumentParser stopQuery("stop");
        stopQuery.add_argument("queryId").scan<'i', size_t>();

        ArgumentParser unregisterQuery("unregister");
        unregisterQuery.add_argument("queryId").scan<'i', size_t>();

        ArgumentParser dump("dump");
        dump.add_argument("-i", "--input").default_value("-").help("Read the query description. Use - for stdin which is the default");

        program.add_subparser(registerQuery);
        program.add_subparser(startQuery);
        program.add_subparser(stopQuery);
        program.add_subparser(unregisterQuery);
        program.add_subparser(dump);

        std::vector<std::reference_wrapper<ArgumentParser>> subcommands{registerQuery, startQuery, stopQuery, unregisterQuery, dump};

        program.parse_args(argc, argv);

        if (program.get<bool>("-d"))
        {
            NES::Logger::getInstance()->changeLogLevel(NES::LogLevel::LOG_DEBUG);
        }

        auto sourceCatalog = std::make_shared<NES::SourceCatalog>();
        auto sinkCatalog = std::make_shared<NES::SinkCatalog>();
        auto workerCatalog = std::make_shared<NES::WorkerCatalog>();
        NES::CLI::YAMLBinder binder{sourceCatalog, sinkCatalog, workerCatalog};
        binder.parseAndBind(std::cin);

        auto queryManager
            = std::make_shared<NES::QueryManager>(std::make_unique<NES::GRPCQuerySubmissionBackend>(std::vector<NES::WorkerConfig>{}));

        const bool anySubcommandUsed
            = std::ranges::any_of(subcommands, [&](auto& subparser) { return program.is_subcommand_used(subparser.get()); });
        if (!anySubcommandUsed)
        {
            std::cerr << "No subcommand used.\n";
            std::cerr << program;
            return 1;
        }

        bool handled = false;
        for (const auto& [name, fn] : std::initializer_list<std::pair<
                 std::string_view,
                 std::expected<void, std::vector<NES::Exception>> (NES::QueryManager::*)(NES::DistributedQueryId)>>{
                 {"start", &NES::QueryManager::start}, {"unregister", &NES::QueryManager::unregister}, {"stop", &NES::QueryManager::stop}})
        {
            if (program.is_subcommand_used(name))
            {
                auto& parser = program.at<ArgumentParser>(name);
                auto queryId = NES::DistributedQueryId{parser.get<size_t>("queryId")};
                ((*queryManager).*fn)(queryId);
                handled = true;
                break;
            }
        }

        if (handled)
        {
            return 0;
        }


        const std::string command = program.is_subcommand_used("register") ? "register" : "dump";
        auto input = program.at<argparse::ArgumentParser>(command).get("-i");
        NES::LogicalPlan boundPlan;
        if (input == "-")
        {
            boundPlan = yamlBinder.parseAndBind(std::cin);
        }
        else
        {
            std::ifstream file{input};
            if (!file)
            {
                throw NES::QueryDescriptionNotReadable(std::strerror(errno)); /// NOLINT(concurrency-mt-unsafe)
            }
            boundPlan = yamlBinder.parseAndBind(file);
        }

        NES::QueryPlanningContext context{
            .id = NES::INVALID<NES::LocalQueryId>,
            .sqlString = boundPlan.getOriginalSql(),
            .sourceCatalog = sourceCatalog,
            .sinkCatalog = sinkCatalog,
            .workerCatalog = workerCatalog};

        auto distributedPlan = NES::QueryPlanner::with(context).plan(NES::PlanStage::BoundLogicalPlan(boundPlan));

        if (program.is_subcommand_used("dump"))
        {
            for (const auto& [worker, plans] : distributedPlan)
            {
                fmt::println("{:#>{}}", "", 80);
                fmt::println("{} plans at {}:", plans.size(), worker);
                for (const auto& plan : plans)
                {
                    fmt::println("{}", plan);
                }
            }
            return 0;
        }

        std::optional<NES::DistributedQueryId> startedQuery;
        if (program.is_subcommand_used("register"))
        {
            auto& registerArgs = program.at<ArgumentParser>("register");
            const auto queryId = queryManager->registerQuery(distributedPlan);
            if (queryId.has_value())
            {
                if (registerArgs.is_used("-x"))
                {
                    if (auto started = queryManager->start(queryId.value()); not started.has_value())
                    {
                        std::cerr << fmt::format(
                            "Could not start query with error {}\n",
                            fmt::join(std::views::transform(started.error(), [](auto ex) { return ex.what(); }), ", "));
                        return 1;
                    }
                    std::cout << queryId.value().getRawValue();
                    startedQuery = std::make_optional(queryId.value());
                }
            }
            else
            {
                std::cerr << std::format("Could not register query: {}\n", queryId.error().what());
                return 1;
            }
        }

        return 0;
    }

    CPPTRACE_CATCH(...)
    {
        NES::tryLogCurrentException();
        return NES::getCurrentErrorCode();
    }
}
