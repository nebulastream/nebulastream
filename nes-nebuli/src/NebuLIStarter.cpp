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

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <thread>
#include <utility>
#include <unistd.h>

#include <QueryManager/GRPCQueryManager.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>

#include <QueryManager/QueryManager.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <argparse/argparse.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <LegacyOptimizer.hpp>
#include <Repl.hpp>
#include <StatementHandler.hpp>
#include <utils.hpp>

#ifdef EMBED_ENGINE
    #include <vector>
    #include <Configurations/Util.hpp>
    #include <QueryManager/EmbeddedWorkerQueryManager.hpp>
    #include <SingleNodeWorkerConfiguration.hpp>
#endif

int runRepl(
    bool interactiveMode,
    const NES::StatementOutputFormat& defaultOutputFormat,
    const NES::ErrorBehaviour& errorBehaviour,
    const std::shared_ptr<NES::QueryManager>& queryManager,
    bool waitForQueryTermination);

int main(int argc, char** argv)
{
    CPPTRACE_TRY
    {
        using argparse::ArgumentParser;
        ArgumentParser program("nebuli");
        program.add_argument("-d", "--debug").flag().help("Dump the query plan and enable debug logging");
        program.add_argument("-w", "--wait").help("Wait for the query to finish").flag();
        program.add_argument("-e", "--error-behaviour")
            .choices("FAIL_FAST", "RECOVER", "CONTINUE_AND_FAIL")
            .help(
                "Fail and return non-zero exit code on first error, ignore error and continue, or continue and return non-zero exit code");
        program.add_argument("-f").default_value("TEXT").choices("TEXT", "JSON").help("Output format");
        program.add_argument("-i", "--input").default_value("-").help("Read the query description. Use - for stdin which is the default");

#ifdef EMBED_ENGINE
        /// single node worker config
        program.add_argument("--")
            .help("arguments passed to the worker config, e.g., `-- --worker.queryEngine.numberOfWorkerThreads=10`")
            .default_value(std::vector<std::string>{})
            .remaining();
#else
        program.add_argument("-s", "--server").required().help("Server URI to connect to").default_value(std::string{"localhost:8080"});
#endif

        program.parse_args(argc, argv);


        bool interactiveMode = false;
        if (program.get<std::string>("-i") == "-")
        {
            /// read from stdin
            interactiveMode
                = static_cast<int>(cpptrace::isatty(STDIN_FILENO)) != 0 and static_cast<int>(cpptrace::isatty(STDOUT_FILENO)) != 0;
        }
        else
        {
            /// read from file. We redirect the file to stdin which makes implementing the repl easier;
            auto inputFilePath = program.get<std::string>("-i");
            if (freopen(inputFilePath.c_str(), "r", stdin) == nullptr)
            {
                fmt::println(std::cerr, "Could not open input file: {}", inputFilePath);
                return 1;
            }
        }

        NES::Logger::setupLogging("nebuli.log", NES::LogLevel::LOG_ERROR, !interactiveMode);

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

        auto queryManager = std::make_shared<NES::EmbeddedWorkerQueryManager>(singleNodeWorkerConfig);

#else
        auto queryManager
            = std::make_shared<NES::GRPCQueryManager>(CreateChannel(program.get<std::string>("-s"), grpc::InsecureChannelCredentials()));

#endif


        return runRepl(interactiveMode, defaultOutputFormat, errorBehaviour, queryManager, program.is_used("-w"));
    }
    CPPTRACE_CATCH(...)
    {
        NES::tryLogCurrentException();
        return NES::getCurrentErrorCode();
    }
}

int runRepl(
    bool interactiveMode,
    const NES::StatementOutputFormat& defaultOutputFormat,
    const NES::ErrorBehaviour& errorBehaviour,
    const std::shared_ptr<NES::QueryManager>& queryManager,
    bool waitForTermination)
{
    auto sourceCatalog = std::make_shared<NES::SourceCatalog>();
    auto sinkCatalog = std::make_shared<NES::SinkCatalog>();
    auto optimizer = std::make_shared<NES::CLI::LegacyOptimizer>(sourceCatalog, sinkCatalog);
    auto binder = NES::StatementBinder{
        sourceCatalog,
        sinkCatalog,
        [](auto&& pH1) { return NES::AntlrSQLQueryParser::bindLogicalQueryPlan(std::forward<decltype(pH1)>(pH1)); }};

    NES::SourceStatementHandler sourceStatementHandler{sourceCatalog};
    NES::SinkStatementHandler sinkStatementHandler{sinkCatalog};
    auto queryStatementHandler = std::make_shared<NES::QueryStatementHandler>(queryManager, optimizer);

    NES::Repl replClient(
        std::move(sourceStatementHandler),
        std::move(sinkStatementHandler),
        queryStatementHandler,
        std::move(binder),
        errorBehaviour,
        defaultOutputFormat,
        interactiveMode);
    replClient.run();

    if (waitForTermination)
    {
        fmt::println(std::cout, "Running queries: {}", queryStatementHandler->getRunningQueries().size());
        for (const auto& query : queryStatementHandler->getRunningQueries())
        {
            auto statusResult = queryManager->status(query);

            if (!statusResult.has_value())
            {
                fmt::println(std::cerr, "Could not retrieve status for query {}. Error: {}", query, statusResult.error());
                continue;
            }

            auto status = statusResult.value();

            while (status.currentStatus != NES::QueryStatus::Stopped && status.currentStatus != NES::QueryStatus::Failed)
            {
                fmt::println(std::cout, "status: {}", magic_enum::enum_name(status.currentStatus));
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                auto nextStatusResult = queryManager->status(query);
                if (!nextStatusResult.has_value())
                {
                    fmt::println(std::cerr, "Could not retrieve status for query {}. Error: {}", query, nextStatusResult.error());
                    break;
                }
                status = nextStatusResult.value();
            }
        }
    }
    return 0;
}
