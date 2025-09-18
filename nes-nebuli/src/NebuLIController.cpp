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
// #include <format>
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

#include <Plans/LogicalPlan.hpp>
#include <QueryManager/GRPCQueryManager.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>

#include <Identifiers/Identifiers.hpp>
#include <QueryManager/QueryManager.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <YAML/YAMLBinder.hpp>
#include <argparse/argparse.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <google/protobuf/text_format.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <magic_enum/magic_enum.hpp>
#include <yaml-cpp/yaml.h>
#include <ErrorHandling.hpp>
#include <LegacyOptimizer.hpp>
#include <SingleNodeWorkerRPCService.grpc.pb.h>
#include <StatementHandler.hpp>
#include <utils.hpp>
#include <Repl.hpp>


int main(int argc, char** argv)
{
    CPPTRACE_TRY
    {
        bool interactiveMode
            = static_cast<int>(cpptrace::isatty(STDIN_FILENO)) != 0 and static_cast<int>(cpptrace::isatty(STDOUT_FILENO)) != 0;

        NES::Logger::setupLogging("nebuli.log", NES::LogLevel::LOG_ERROR, !interactiveMode);

        using argparse::ArgumentParser;
        ArgumentParser program("nebuli-controller");
        program.add_argument("-d", "--debug").flag().help("Dump the query plan and enable debug logging");
        program.add_argument("-s", "--server").help("Server URI to connect to").default_value(std::string{"localhost:8080"});
        // removed: wait for query to finish option (-w/--wait)

        program.add_argument("-e", "--error-behaviour")
            .choices("FAIL_FAST", "RECOVER", "CONTINUE_AND_FAIL")
            .help(
                "Fail and return non-zero exit code on first error, ignore error and continue, or continue and return non-zero exit code");
        program.add_argument("-f").default_value("TEXT").choices("TEXT", "JSON").help("Output format");

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
        dump.add_argument("-o", "--output").default_value("-").help("Write the DecomposedQueryPlan to file. Use - for stdout");
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

        const auto defaultOutputFormatOpt = magic_enum::enum_cast<NES::StatementOutputFormat>(program.get<std::string>("-f"));
        if (not defaultOutputFormatOpt.has_value())
        {
            NES_ERROR("Invalid output format: {}", program.get<std::string>("-f"));
            return 1;
        }
        const auto defaultOutputFormat = defaultOutputFormatOpt.value();


        const NES::ErrorBehaviour errorBehaviour = [&]
        {
            if (program.is_used("-e"))
            {
                auto errorBehaviourOpt = magic_enum::enum_cast<NES::ErrorBehaviour>(program.get<std::string>("-e"));
                if (not errorBehaviourOpt.has_value())
                {
                    throw NES::InvalidConfigParameter(
                        "Error behaviour must be set to FAIL_FAST, RECOVER or CONTINUE_AND_FAIL, but was set to {}",
                        program.get<std::string>("-e"));
                }
                return errorBehaviourOpt.value();
            }
            if (interactiveMode)
            {
                return NES::ErrorBehaviour::RECOVER;
            }
            return NES::ErrorBehaviour::FAIL_FAST;
        }();


        auto sourceCatalog = std::make_shared<NES::SourceCatalog>();
        auto sinkCatalog = std::make_shared<NES::SinkCatalog>();
        auto yamlBinder = NES::CLI::YAMLBinder{sourceCatalog, sinkCatalog};
        auto optimizer = std::make_shared<NES::CLI::LegacyOptimizer>(sourceCatalog, sinkCatalog);
        std::shared_ptr<NES::QueryManager> queryManager{};

        // The controller should always connect to a remote server
        if (program.is_used("-s"))
        {
            queryManager = std::make_shared<NES::GRPCQueryManager>(
                CreateChannel(program.get<std::string>("-s"), grpc::InsecureChannelCredentials()));
        }
        else
        {
            NES_ERROR("No server address given. Please use the -s option to specify the server address.")
            return 1;
        }

        std::optional<NES::QueryId> startedQuery;
        bool handled = false;
        for (const auto& [name, fn] :
             std::initializer_list<std::pair<std::string_view, std::expected<void, NES::Exception> (NES::QueryManager::*)(NES::QueryId)>>{
                 {"start", &NES::QueryManager::start}, {"unregister", &NES::QueryManager::unregister}, {"stop", &NES::QueryManager::stop}})
        {
            if (program.is_subcommand_used(name))
            {
                auto& parser = program.at<argparse::ArgumentParser>(name); // Fully qualify ArgumentParser
                auto queryId = NES::QueryId{parser.get<size_t>("queryId")};
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

        const NES::LogicalPlan optimizedQueryPlan = optimizer->optimize(boundPlan);

        std::string output;
        auto serialized = NES::QueryPlanSerializationUtil::serializeQueryPlan(optimizedQueryPlan);
        google::protobuf::TextFormat::PrintToString(serialized, &output);
        NES_INFO("GRPC QueryPlan: {}", output);
        if (program.is_subcommand_used("dump"))
        {
            auto& dumpArgs = program.at<argparse::ArgumentParser>("dump");
            auto outputPath = dumpArgs.get<std::string>("-o");
            std::ostream* output = nullptr;
            std::ofstream file;
            if (outputPath == "-")
            {
                output = &std::cout;
            }
            else
            {
                file = std::ofstream(outputPath);
                if (!file)
                {
                    throw NES::UnknownException("Could not open output file: {}", outputPath);
                }
                output = &file;
            }
            *output << serialized.DebugString() << '\n';

            if (outputPath == "-")
            {
                NES_INFO("Wrote protobuf to {}", outputPath);
            }
        }

        if (program.is_subcommand_used("register"))
        {
            auto& registerArgs = program.at<argparse::ArgumentParser>("register");
            const auto queryId = queryManager->registerQuery(optimizedQueryPlan);
            if (queryId.has_value())
            {
                if (registerArgs.is_used("-x"))
                {
                    if (auto started = queryManager->start(queryId.value()); not started.has_value())
                    {
                        std::cerr << fmt::format("Could not start query with error {}\n", started.error().what());
                        return 1;
                    }
                    std::cout << queryId.value().getRawValue();
                    startedQuery = std::make_optional(queryId.value());
                }
            }
            else
            {
                fmt::println(std::cerr, "Could not register query: {}", queryId.error().what());
                return 1;
            }
        }

        // removed: waiting for query termination controlled by -w

        return 0;
    }
    CPPTRACE_CATCH(...)
    {
        NES::tryLogCurrentException();
        return NES::getCurrentErrorCode();
    }
}
