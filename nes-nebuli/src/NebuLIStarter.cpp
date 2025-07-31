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
#include <cstddef>
#include <cstring>
#include <fstream>
#include <functional>
#include <initializer_list>
#include <iostream>
#include <memory>
#include <ostream>
#include <utility>
#include <vector>

#include <Plans/LogicalPlan.hpp>
#include <QueryManager/GRPCQueryManager.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>

#include <Identifiers/Identifiers.hpp>
#include <QueryManager/QueryManager.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <YAML/YAMLBinder.hpp>
#include <argparse/argparse.hpp>
#include <cpptrace/from_current.hpp>
#include <google/protobuf/text_format.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <magic_enum/magic_enum.hpp>
#include <yaml-cpp/yaml.h>
#include <ErrorHandling.hpp>
#include <LegacyOptimizer.hpp>
#include <Repl.hpp>
#include <SingleNodeWorkerRPCService.grpc.pb.h>

#ifdef EMBED_ENGINE
    #include <Configurations/Util.hpp>
    #include <QueryManager/EmbeddedWorkerQueryManager.hpp>
    #include <SingleNodeWorkerConfiguration.hpp>
#endif


int main(int argc, char** argv)
{
    CPPTRACE_TRY
    {
        NES::Logger::setupLogging("nebuli.log", NES::LogLevel::LOG_ERROR);
        using argparse::ArgumentParser;
        ArgumentParser program("nebuli");
        program.add_argument("-d", "--debug").flag().help("Dump the query plan and enable debug logging");
        program.add_argument("-s", "--server").help("Server URI to connect to").default_value(std::string{"localhost:8080"});
        /// single node worker config
        program.add_argument("--")
            .help("arguments passed to the worker config, e.g., `-- --worker.queryEngine.numberOfWorkerThreads=10`")
            .default_value(std::vector<std::string>{})
            .remaining();
        ArgumentParser registerQuery("register");
        registerQuery.add_argument("-i", "--input")
            .default_value("-")
            .help("Read the query description. Use - for stdin which is the default");
        registerQuery.add_argument("-x").help("Immediately start the query as well").flag();
        registerQuery.add_argument("-w", "--wait").help("Wait for the query to finish").flag();

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
        std::shared_ptr<NES::QueryManager> queryManager{};

        if (program.is_used("-s"))
        {
            queryManager = std::make_shared<NES::GRPCQueryManager>(
                CreateChannel(program.get<std::string>("-s"), grpc::InsecureChannelCredentials()));
        }
        else
        {
#ifdef EMBED_ENGINE
            auto confVec = program.get<std::vector<std::string>>("--");
            if ((confVec.size() + 1) > INT_MAX)
            {
                NES_ERROR("Too many worker configuration options passed through, maximum is {}", INT_MAX);
                return 1;
            }
            const int singleNodeArgC = static_cast<int>(confVec.size() + 1);
            std::vector<const char*> singleNodeArgV;
            singleNodeArgV.reserve(singleNodeArgC + 1);
            singleNodeArgV.push_back("systest"); /// dummy option as arg expects first arg to be the program name
            for (auto& arg : confVec)
            {
                singleNodeArgV.push_back(arg.c_str());
            }
            auto singleNodeWorkerConfig = NES::loadConfiguration<NES::SingleNodeWorkerConfiguration>(singleNodeArgC, singleNodeArgV.data())
                                              .value_or(NES::SingleNodeWorkerConfiguration{});

            queryManager = std::make_shared<NES::EmbeddedWorkerQueryManager>(singleNodeWorkerConfig);
#else
            NES_ERROR("No server address given. Please use the -s option to specify the server address or use nebuli-embedded to start a "
                      "single node worker.")
            return 1;
#endif
        }

        const bool anySubcommandUsed
            = std::ranges::any_of(subcommands, [&](auto& subparser) { return program.is_subcommand_used(subparser.get()); });
        if (!anySubcommandUsed)
        {
            NES::Repl replClient(queryManager);
            replClient.run();
            return 0;
        }


        bool handled = false;
        for (const auto& [name, fn] :
             std::initializer_list<std::pair<std::string_view, std::expected<void, NES::Exception> (NES::QueryManager::*)(NES::QueryId)>>{
                 {"start", &NES::QueryManager::start}, {"unregister", &NES::QueryManager::unregister}, {"stop", &NES::QueryManager::stop}})
        {
            if (program.is_subcommand_used(name))
            {
                auto& parser = program.at<ArgumentParser>(name);
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

        auto sourceCatalog = std::make_shared<NES::SourceCatalog>();
        auto sinkCatalog = std::make_shared<NES::SinkCatalog>();
        auto yamlBinder = NES::CLI::YAMLBinder{sourceCatalog, sinkCatalog};
        auto optimizer = NES::CLI::LegacyOptimizer{sourceCatalog, sinkCatalog};

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

        const NES::LogicalPlan optimizedQueryPlan = optimizer.optimize(boundPlan);

        std::string output;
        auto serialized = NES::QueryPlanSerializationUtil::serializeQueryPlan(optimizedQueryPlan);
        google::protobuf::TextFormat::PrintToString(serialized, &output);
        NES_INFO("GRPC QueryPlan: {}", output);
        if (program.is_subcommand_used("dump"))
        {
            auto& dumpArgs = program.at<ArgumentParser>("dump");
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
        else if (program.is_subcommand_used("register"))
        {
            auto& registerArgs = program.at<ArgumentParser>("register");
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
                    if (registerArgs.is_used("-w"))
                    {
                        auto status = queryManager->status(queryId.value());
                        while (status.has_value()
                               && !(
                                   status.value().currentStatus == NES::QueryStatus::Stopped
                                   || status.value().currentStatus == NES::QueryStatus::Failed))
                        {
                            std::this_thread::sleep_for(std::chrono::milliseconds(50));
                            status = queryManager->status(queryId.value());
                        }
                    }
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
