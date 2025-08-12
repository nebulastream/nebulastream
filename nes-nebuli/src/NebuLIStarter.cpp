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
#include <chrono>
#include <exception>
#include <fstream>
#include <functional>
#include <iostream>
#include <memory>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include <argparse/argparse.hpp>
#include <google/protobuf/text_format.h>
#include <grpcpp/create_channel.h>
#include <from_current.hpp>

#include <Distributed/DistributedQueryId.hpp>
#include <Distributed/WorkerStatus.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <YAML/YAMLBinder.hpp>
#include <ErrorHandling.hpp>
#include <GRPCClient.hpp>
#include <QueryPlanning.hpp>
#include <Repl.hpp>

namespace
{
/// constants for command names and arguments
/// command names
constexpr auto CMD_REGISTER = "register";
constexpr auto CMD_START = "start";
constexpr auto CMD_STOP = "stop";
constexpr auto CMD_UNREGISTER = "unregister";
constexpr auto CMD_DUMP = "dump";
constexpr auto CMD_STATUS = "status";
constexpr auto CMD_QUERY_STATUS = "query";

/// argument names
constexpr auto ARG_DEBUG_SHORT = "-d";
constexpr auto ARG_DEBUG_LONG = "--debug";
constexpr auto ARG_INPUT_SHORT = "-i";
constexpr auto ARG_INPUT_LONG = "--input";
constexpr auto ARG_OUTPUT_SHORT = "-o";
constexpr auto ARG_OUTPUT_LONG = "--output";
constexpr auto ARG_AUTO_START = "-x";
constexpr auto ARG_AFTER = "--after";
constexpr auto ARG_QUERY_ID = "queryId";

/// default values
constexpr auto DEFAULT_STDIN_STDOUT = "-";
constexpr auto DEFAULT_LOG_FILE = "client.log";
constexpr auto PROGRAM_NAME = "nebuli";

/// =================================== Command Handlers ===================================
void handleStatus(const argparse::ArgumentParser& statusArgs)
{
    const auto inputPath = statusArgs.get<std::string>(ARG_INPUT_SHORT);
    const auto queryConfig = NES::CLI::YAMLLoader::load(inputPath);

    auto after = std::chrono::system_clock::time_point(std::chrono::milliseconds(0));
    if (statusArgs.is_used(ARG_AFTER))
    {
        after = std::chrono::system_clock::time_point(std::chrono::milliseconds(statusArgs.get<size_t>(ARG_AFTER)));
    }

    for (const auto& workerConfig : queryConfig.workerNodes)
    {
        try
        {
            auto channel = grpc::CreateChannel(workerConfig.grpc, grpc::InsecureChannelCredentials());
            NES::GRPCClient client(std::move(channel));

            auto summary = client.summary(after);
            NES::Distributed::prettyPrintWorkerStatus(workerConfig.grpc, summary);
        }
        catch (const std::exception& e)
        {
            NES_ERROR("Failed to get summary of node at {}: {}", workerConfig.grpc, e.what());
        }
        catch (...)
        {
            NES::tryLogCurrentException();
            NES_ERROR("Unknown error getting summary of node at {}", workerConfig.grpc);
        }
        fmt::print("\n");
    }
}

void handleDump(const argparse::ArgumentParser& dumpArgs, const NES::LogicalPlan& plan)
{
    /// Serialize the plan
    auto serialized = NES::QueryPlanSerializationUtil::serializeQueryPlan(plan);

    std::string planString;
    if (!google::protobuf::TextFormat::PrintToString(serialized, &planString))
    {
        throw std::runtime_error("Failed to serialize query plan to string");
    }
    NES_INFO("Plan fragment: {}", planString);

    if (const auto outputPath = dumpArgs.get<std::string>(ARG_OUTPUT_SHORT); outputPath == DEFAULT_STDIN_STDOUT)
    {
        std::cout << serialized.DebugString() << '\n';
        NES_INFO("Wrote protobuf to stdout");
    }
    else
    {
        std::ofstream file(outputPath);
        if (!file)
        {
            throw std::runtime_error(fmt::format("Could not open output file: {}", outputPath));
        }
        file << serialized.DebugString() << '\n';
        NES_INFO("Wrote protobuf to {}", outputPath);
    }
}

template <typename Func>
void executeForEachQuery(const std::string& queryIdStr, Func&& func)
{
    for (auto [queries] = NES::Distributed::QueryId::load(queryIdStr); const auto& [connection, queryId] : queries)
    {
        try
        {
            auto channel = grpc::CreateChannel(connection, grpc::InsecureChannelCredentials());
            auto client = std::make_shared<NES::GRPCClient>(std::move(channel));
            func(client, NES::QueryId{queryId}, connection);
            NES_INFO("Successfully executed command for query {} on host {}", queryId, connection);
        }
        catch (const std::exception& e)
        {
            NES_ERROR("Failed to execute command for query {} on host {}: {}", queryId, connection, e.what());
            throw;
        }
    }
}

void handleStart(const argparse::ArgumentParser& startArgs)
{
    executeForEachQuery(
        startArgs.get<std::string>(ARG_QUERY_ID), [](auto& client, const auto& queryId, const auto&) { client->start(queryId); });
}

void handleQueryStatus(const argparse::ArgumentParser& statusArgs)
{
    const bool perWorker = statusArgs.get<bool>("--per-worker");
    std::vector<NES::QueryStatus> queryStatuses;
    executeForEachQuery(
        statusArgs.get<std::string>(ARG_QUERY_ID),
        [&](std::shared_ptr<NES::GRPCClient>& client, const auto& queryId, const auto& connection)
        {
            auto status = client->status(queryId);
            if (perWorker)
            {
                fmt::println(
                    std::cout, "Query {} on worker {}: {}", queryId.getRawValue(), connection, magic_enum::enum_name(status.currentStatus));
            }
            queryStatuses.push_back(status.currentStatus);
        });

    auto running = std::ranges::count_if(queryStatuses, [](const auto& qs) { return qs == NES::QueryStatus::Running; });
    auto stopped = std::ranges::count_if(queryStatuses, [](const auto& qs) { return qs == NES::QueryStatus::Stopped; });
    auto failed = std::ranges::count_if(queryStatuses, [](const auto& qs) { return qs == NES::QueryStatus::Failed; });
    if (failed > 0)
    {
        fmt::println(std::cout, "FAILED");
    }
    else if (stopped == 0 && running > 0)
    {
        fmt::println(std::cout, "RUNNING");
    }
    else if (stopped > 0 && running > 0)
    {
        fmt::println(std::cout, "PARTIALLY RUNNING");
    }
    else if (stopped > 0 && running == 0)
    {
        fmt::println(std::cout, "COMPLETED");
    }
}

void handleStop(const argparse::ArgumentParser& stopArgs)
{
    executeForEachQuery(stopArgs.get<std::string>(ARG_QUERY_ID), [](auto& client, const auto& queryId, const auto&) { client->stop(queryId); });
}

void handleUnregister(const argparse::ArgumentParser& unregisterArgs)
{
    executeForEachQuery(
        unregisterArgs.get<std::string>(ARG_QUERY_ID), [](auto& client, const auto& queryId, const auto&) { client->unregister(queryId); });
}

void handleRegister(const argparse::ArgumentParser& registerArgs, const NES::QueryDecomposer::DecomposedLogicalPlan& decomposedPlan)
{
    std::vector<NES::Distributed::QueryId::ConnectionQueryIdPair> queryFragments;
    const bool autoStart = registerArgs.is_used(ARG_AUTO_START);

    for (const auto& [grpcAddr, plans] : decomposedPlan)
    {
        try
        {
            auto channel
                = grpc::CreateChannel(grpcAddr, grpc::InsecureChannelCredentials()); /// TODO: use grpc address instead of host addr
            const auto client = std::make_shared<NES::GRPCClient>(std::move(channel));

            for (const auto& plan : plans)
            {
                const auto queryId = client->registerQuery(plan);
                queryFragments.emplace_back(grpcAddr, queryId.getRawValue());

                if (autoStart)
                {
                    client->start(queryId);
                    NES_INFO("Started query {} on host {}", queryId.getRawValue(), grpcAddr);
                }
            }
        }
        catch (const std::exception& e)
        {
            NES_ERROR("Failed to register query on host {}: {}", grpcAddr, e.what());
            throw;
        }
    }

    auto id = NES::Distributed::QueryId::save(NES::Distributed::QueryId{queryFragments});
    std::cout << id << '\n';
    NES_INFO("Successfully registered {} query fragments", queryFragments.size());
}

/// =================================== Argument Parsers ===================================
struct ArgParsers
{
    argparse::ArgumentParser registerParser{CMD_REGISTER};
    argparse::ArgumentParser startParser{CMD_START};
    argparse::ArgumentParser stopParser{CMD_STOP};
    argparse::ArgumentParser unregisterParser{CMD_UNREGISTER};
    argparse::ArgumentParser dumpParser{CMD_DUMP};
    argparse::ArgumentParser statusParser{CMD_STATUS};
    argparse::ArgumentParser queryStatusParser{CMD_QUERY_STATUS};
};

void setupArgParsers(argparse::ArgumentParser& program, ArgParsers& parsers)
{
    /// Main parser arguments
    program.add_argument(ARG_DEBUG_SHORT, ARG_DEBUG_LONG).flag().help("Dump the query plan and enable debug logging");

    /// Register subcommand
    parsers.registerParser.add_argument(ARG_INPUT_SHORT, ARG_INPUT_LONG)
        .default_value(std::string(DEFAULT_STDIN_STDOUT))
        .help("Read the query description. Use - for stdin, which is the default");
    parsers.registerParser.add_argument(ARG_AUTO_START).flag().help("Automatically start the query after registration");
    program.add_subparser(parsers.registerParser);

    /// Start subcommand
    parsers.startParser.add_argument(ARG_QUERY_ID).help("ID of the query to start");
    program.add_subparser(parsers.startParser);

    /// Stop subcommand
    parsers.stopParser.add_argument(ARG_QUERY_ID).help("ID of the query to stop");
    program.add_subparser(parsers.stopParser);

    /// Unregister subcommand
    parsers.unregisterParser.add_argument(ARG_QUERY_ID).help("ID of the query to unregister");
    program.add_subparser(parsers.unregisterParser);

    /// Dump subcommand
    parsers.dumpParser.add_argument(ARG_OUTPUT_SHORT, ARG_OUTPUT_LONG)
        .default_value(std::string(DEFAULT_STDIN_STDOUT))
        .help("Write the query plan to file. Use - for stdout");
    parsers.dumpParser.add_argument(ARG_INPUT_SHORT, ARG_INPUT_LONG)
        .default_value(std::string(DEFAULT_STDIN_STDOUT))
        .help("Read the query description. Use - for stdin which is the default");
    program.add_subparser(parsers.dumpParser);

    /// Status subcommand
    parsers.statusParser.add_argument(ARG_INPUT_SHORT).nargs(1).help("Path to the query configuration file");
    parsers.statusParser.add_argument(ARG_AFTER).scan<'i', size_t>().default_value(size_t{0}).help(
        "Request only status updates after this unix timestamp");
    program.add_subparser(parsers.statusParser);

    /// QueryStatus subcommand
    parsers.queryStatusParser.add_argument(ARG_QUERY_ID).required();
    parsers.queryStatusParser.add_argument("-p", "--per-worker").flag().help("Show per worker status instead of combined status");
    program.add_subparser(parsers.queryStatusParser);
}
}

int main(const int argc, char** argv)
{
    CPPTRACE_TRY
    {
        /// Initialize logging
        NES::Logger::setupLogging(DEFAULT_LOG_FILE, NES::LogLevel::LOG_ERROR);

        /// Setup argument parser
        argparse::ArgumentParser program{PROGRAM_NAME};
        ArgParsers parsers{};
        try
        {
            setupArgParsers(program, parsers);
            program.parse_args(argc, argv);
        }
        catch (const std::exception& e)
        {
            std::cerr << "Error parsing arguments: " << e.what() << '\n';
            std::cerr << program;
            return 1;
        }

        std::vector<std::reference_wrapper<argparse::ArgumentParser>> subcommands{
            parsers.registerParser,
            parsers.startParser,
            parsers.stopParser,
            parsers.unregisterParser,
            parsers.dumpParser,
            parsers.statusParser,
            parsers.queryStatusParser};

        if (!std::ranges::any_of(subcommands, [&](auto& subparser) { return program.is_subcommand_used(subparser.get()); }))
        {
            /// TODO(yschroeder97): fix REPL
            const auto client = std::make_shared<NES::GRPCClient>(CreateChannel("localhost:8080", grpc::InsecureChannelCredentials()));
            NES::Repl replClient(client);
            replClient.run();
            return 0;
        }

        /// Enable debug logging if requested
        if (program.get<bool>(ARG_DEBUG_LONG))
        {
            NES::Logger::getInstance()->changeLogLevel(NES::LogLevel::LOG_DEBUG);
        }

        /// Handle subcommands
        try
        {
            if (program.is_subcommand_used(CMD_STATUS))
            {
                handleStatus(program.at<argparse::ArgumentParser>(CMD_STATUS));
            }
            else if (program.is_subcommand_used(CMD_QUERY_STATUS))
            {
                handleQueryStatus(program.at<argparse::ArgumentParser>(CMD_QUERY_STATUS));
            }
            else if (program.is_subcommand_used(CMD_START))
            {
                handleStart(program.at<argparse::ArgumentParser>(CMD_START));
            }
            else if (program.is_subcommand_used(CMD_STOP))
            {
                handleStop(program.at<argparse::ArgumentParser>(CMD_STOP));
            }
            else if (program.is_subcommand_used(CMD_UNREGISTER))
            {
                handleUnregister(program.at<argparse::ArgumentParser>(CMD_UNREGISTER));
            }
            else if (program.is_subcommand_used(CMD_REGISTER) || program.is_subcommand_used(CMD_DUMP))
            {
                /// Both register and dump need to prepare the query plan
                const std::string command = program.is_subcommand_used(CMD_REGISTER) ? CMD_REGISTER : CMD_DUMP;
                const auto& subparser = program.at<argparse::ArgumentParser>(command);
                const auto inputPath = subparser.get<std::string>(ARG_INPUT_SHORT);

                auto [decomposedPlan, a, b] = NES::QueryPlanner::plan(inputPath);

                if (command == CMD_DUMP)
                {
                    for (const auto& nodePlan : decomposedPlan | std::views::values)
                    {
                        for (const auto& plan : nodePlan)
                        {
                            handleDump(subparser, plan);
                        }
                    }
                }
                else
                {
                    handleRegister(subparser, decomposedPlan);
                }
            }
            else
            {
                std::cerr << "No command specified\n";
                std::cerr << program;
                return 1;
            }
        }
        catch (const std::exception& e)
        {
            NES_ERROR("Command execution failed: {}", e.what());
            return 1;
        }

        return 0;
    }
    CPPTRACE_CATCH(...)
    {
        NES::tryLogCurrentException();
        return NES::getCurrentErrorCode();
    }
}
