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

#include <Plans/LogicalPlan.hpp>
#include <QueryManager/QueryManager.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>

#include <../repl/Repl.hpp>
#include <GlobalOptimizer/GlobalOptimizer.hpp>
#include <Identifiers/Identifiers.hpp>
#include <QueryManager/GRPCQuerySubmissionBackend.hpp>
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
#include <SingleNodeWorkerRPCService.grpc.pb.h>
#include <StatementHandler.hpp>
#include <utils.hpp>

#ifdef EMBED_ENGINE
    #include <Configurations/Util.hpp>
    #include <QueryManager/EmbeddedWorkerQuerySubmissionBackend.hpp>
    #include <SingleNodeWorkerConfiguration.hpp>
#endif


int main(int argc, char** argv)
{
    CPPTRACE_TRY
    {
        bool interactiveMode
            = static_cast<int>(cpptrace::isatty(STDIN_FILENO)) != 0 and static_cast<int>(cpptrace::isatty(STDOUT_FILENO)) != 0;

        NES::Logger::setupLogging("nebuli.log", NES::LogLevel::LOG_ERROR, !interactiveMode);

        using argparse::ArgumentParser;
        ArgumentParser program("nebuli");
        program.add_argument("-d", "--debug").flag().help("Dump the query plan and enable debug logging");
        program.add_argument("-s", "--server").help("Server URI to connect to").default_value(std::string{"localhost:8080"});
        program.add_argument("-w", "--wait").help("Wait for the query to finish").flag();

        program.add_argument("-e", "--error-behaviour")
            .choices("FAIL_FAST", "RECOVER", "CONTINUE_AND_FAIL")
            .help(
                "Fail and return non-zero exit code on first error, ignore error and continue, or continue and return non-zero exit code");
        program.add_argument("-f").default_value("TEXT").choices("TEXT", "JSON").help("Output format");
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
        auto workerCatalog = std::make_shared<NES::WorkerCatalog>();
        auto yamlBinder = NES::CLI::YAMLBinder{sourceCatalog, sinkCatalog, workerCatalog};
        std::shared_ptr<NES::QueryManager> queryManager{};
        auto binder = NES::StatementBinder{
            sourceCatalog,
            sinkCatalog,
            [](auto&& pH1) { return NES::AntlrSQLQueryParser::bindLogicalQueryPlan(std::forward<decltype(pH1)>(pH1)); }};

        if (program.is_used("-s"))
        {
            queryManager
                = std::make_shared<NES::QueryManager>(std::make_unique<NES::GRPCQuerySubmissionBackend>(std::vector<NES::WorkerConfig>{}));
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

            queryManager = std::make_shared<NES::QueryManager>(
                std::make_unique<NES::EmbeddedWorkerQuerySubmissionBackend>(std::vector<NES::WorkerConfig>{}, singleNodeWorkerConfig));
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
            NES::SourceStatementHandler sourceStatementHandler{sourceCatalog};
            NES::SinkStatementHandler sinkStatementHandler{sinkCatalog};
            auto queryStatementHandler = std::make_shared<NES::QueryStatementHandler>(queryManager, sourceCatalog, sinkCatalog);
            NES::Repl replClient(
                std::move(sourceStatementHandler),
                std::move(sinkStatementHandler),
                queryStatementHandler,
                std::move(binder),
                errorBehaviour,
                defaultOutputFormat,
                interactiveMode);
            replClient.run();

            if (program.is_used("-w"))
            {
                while (!queryManager->getRunningQueries().empty())
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                }
            }
            return 0;
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

        if (program.is_used("-w"))
        {
            while (!queryManager->getRunningQueries().empty())
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
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
