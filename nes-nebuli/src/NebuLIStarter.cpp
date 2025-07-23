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

#include <chrono>
#include <exception>
#include <fstream>
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
#include <Distributed/OperatorPlacement.hpp>
#include <Distributed/QueryDecomposition.hpp>
#include <Distributed/WorkerStatus.hpp>
#include <LegacyOptimizer/LegacyOptimizer.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <YAML/YAMLBinder.hpp>
#include <ErrorHandling.hpp>
#include <GRPCClient.hpp>
#include <QueryPlanning.hpp>

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
    for (auto [queries] = NES::Distributed::QueryId::load(queryIdStr); const auto& [hostAddr, queryId] : queries)
    {
        try
        {
            auto channel = grpc::CreateChannel(hostAddr, grpc::InsecureChannelCredentials());
            auto client = std::make_shared<NES::GRPCClient>(std::move(channel));
            func(client, NES::QueryId{queryId});
            NES_INFO("Successfully executed command for query {} on host {}", queryId, hostAddr);
        }
        catch (const std::exception& e)
        {
            NES_ERROR("Failed to execute command for query {} on host {}: {}", queryId, hostAddr, e.what());
            throw;
        }
    }
}

void handleStart(const argparse::ArgumentParser& startArgs)
{
    executeForEachQuery(startArgs.get<std::string>(ARG_QUERY_ID), [](auto& client, const auto& queryId) { client->start(queryId); });
}

void handleStop(const argparse::ArgumentParser& stopArgs)
{
    executeForEachQuery(stopArgs.get<std::string>(ARG_QUERY_ID), [](auto& client, const auto& queryId) { client->stop(queryId); });
}

void handleUnregister(const argparse::ArgumentParser& unregisterArgs)
{
    executeForEachQuery(
        unregisterArgs.get<std::string>(ARG_QUERY_ID), [](auto& client, const auto& queryId) { client->unregister(queryId); });
}

void handleRegister(const argparse::ArgumentParser& registerArgs, const NES::QueryDecomposer::DecomposedLogicalPlan& decomposedPlan)
{
    std::vector<NES::Distributed::QueryId::ConnectionQueryIdPair> queryFragments;
    const bool autoStart = registerArgs.is_used(ARG_AUTO_START);

    for (const auto& [hostAddr, plan] : decomposedPlan)
    {
        try
        {
            auto channel = grpc::CreateChannel(hostAddr, grpc::InsecureChannelCredentials()); /// TODO: use grpc address instead of host addr
            const auto client = std::make_shared<NES::GRPCClient>(std::move(channel));

            const auto queryId = client->registerQuery(plan);
            queryFragments.emplace_back(hostAddr, queryId.getRawValue());

            if (autoStart)
            {
                client->start(queryId);
                NES_INFO("Started query {} on host {}", queryId.getRawValue(), hostAddr);
            }

            std::cout << queryId.getRawValue() << '\n';
        }
        catch (const std::exception& e)
        {
            NES_ERROR("Failed to register query on host {}: {}", hostAddr, e.what());
            throw;
        }
    }

    NES::Distributed::QueryId::save(NES::Distributed::QueryId{queryFragments});
    NES_INFO("Successfully registered {} query fragments", queryFragments.size());
}

/// =================================== Argument Parsers ===================================
void setupArgParsers(argparse::ArgumentParser& program)
{
    /// Main parser arguments
    program.add_argument(ARG_DEBUG_SHORT, ARG_DEBUG_LONG).flag().help("Dump the query plan and enable debug logging");

    /// Register subcommand
    {
        argparse::ArgumentParser registerParser(CMD_REGISTER);
        registerParser.add_argument(ARG_INPUT_SHORT, ARG_INPUT_LONG)
            .default_value(std::string(DEFAULT_STDIN_STDOUT))
            .help("Read the query description. Use - for stdin, which is the default");
        registerParser.add_argument(ARG_AUTO_START).flag().help("Automatically start the query after registration");
        program.add_subparser(registerParser);
    }

    /// Start subcommand
    {
        argparse::ArgumentParser startParser(CMD_START);
        startParser.add_argument(ARG_QUERY_ID).scan<'i', size_t>().help("ID of the query to start");
        program.add_subparser(startParser);
    }

    /// Stop subcommand
    {
        argparse::ArgumentParser stopParser(CMD_STOP);
        stopParser.add_argument(ARG_QUERY_ID).scan<'i', size_t>().help("ID of the query to stop");
        program.add_subparser(stopParser);
    }

    /// Unregister subcommand
    {
        argparse::ArgumentParser unregisterParser(CMD_UNREGISTER);
        unregisterParser.add_argument(ARG_QUERY_ID).scan<'i', size_t>().help("ID of the query to unregister");
        program.add_subparser(unregisterParser);
    }

    /// Dump subcommand
    {
        argparse::ArgumentParser dumpParser(CMD_DUMP);
        dumpParser.add_argument(ARG_OUTPUT_SHORT, ARG_OUTPUT_LONG)
            .default_value(std::string(DEFAULT_STDIN_STDOUT))
            .help("Write the query plan to file. Use - for stdout");
        dumpParser.add_argument(ARG_INPUT_SHORT, ARG_INPUT_LONG)
            .default_value(std::string(DEFAULT_STDIN_STDOUT))
            .help("Read the query description. Use - for stdin which is the default");
        program.add_subparser(dumpParser);
    }

    /// Status subcommand
    {
        argparse::ArgumentParser statusParser(CMD_STATUS);
        statusParser.add_argument(ARG_INPUT_SHORT).nargs(1).help("Path to the query configuration file");
        statusParser.add_argument(ARG_AFTER).scan<'i', size_t>().default_value(size_t{0}).help(
            "Request only status updates after this unix timestamp");
        program.add_subparser(statusParser);
    }
}
}

int main(const int argc, char** argv)
{
    CPPTRACE_TRY
    {
        /// Initialize logging
        NES::Logger::setupLogging(DEFAULT_LOG_FILE, NES::LogLevel::LOG_ERROR);

        /// Setup argument parser
        argparse::ArgumentParser program(PROGRAM_NAME);
        setupArgParsers(program);

        try
        {
            program.parse_args(argc, argv);
        }
        catch (const std::exception& e)
        {
            std::cerr << "Error parsing arguments: " << e.what() << '\n';
            std::cerr << program;
            return 1;
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

                auto decomposedPlan = NES::QueryPlanner::plan(inputPath);

                if (command == CMD_DUMP)
                {
                    for (const auto& nodePlan : decomposedPlan | std::views::values)
                    {
                        handleDump(subparser, nodePlan);
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