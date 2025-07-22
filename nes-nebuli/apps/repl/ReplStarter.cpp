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
#include <functional>
#include <memory>
#include <optional>
#include <thread>
#include <utility>
#include <vector>
#include <unistd.h>

#include <QueryManager/QueryManager.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>

#include <Identifiers/Identifiers.hpp>
#include <QueryManager/GRPCQuerySubmissionBackend.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Statements/StatementHandler.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/Pointers.hpp>
#include <argparse/argparse.hpp>
#include <cpptrace/from_current.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>
#include <Repl.hpp>
#include <Thread.hpp>
#include <utils.hpp>

#ifdef EMBED_ENGINE
    #include <Configurations/Util.hpp>
    #include <QueryManager/EmbeddedWorkerQuerySubmissionBackend.hpp>
    #include <SingleNodeWorkerConfiguration.hpp>
#endif

namespace
{
constexpr NES::HostAddr hostAddr{"localhost:9090"};
constexpr NES::GrpcAddr grpcAddr{"localhost:8080"};

NES::UniquePtr<NES::GRPCQuerySubmissionBackend> createGRPCBackend()
{
    return std::make_unique<NES::GRPCQuerySubmissionBackend>(NES::WorkerConfig{.host = hostAddr, .grpc = grpcAddr});
}
}

int main(int argc, char** argv)
{
    CPPTRACE_TRY
    {
        bool interactiveMode
            = static_cast<int>(cpptrace::isatty(STDIN_FILENO)) != 0 and static_cast<int>(cpptrace::isatty(STDOUT_FILENO)) != 0;

        NES::Thread::initializeThread(NES::WorkerId("nebuli"), "main");
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
        std::shared_ptr<NES::QueryManager> queryManager{};
        auto binder = NES::StatementBinder{
            sourceCatalog, [](auto&& pH1) { return NES::AntlrSQLQueryParser::bindLogicalQueryPlan(std::forward<decltype(pH1)>(pH1)); }};

        if (program.is_used("-s"))
        {
            queryManager = std::make_shared<NES::QueryManager>(createGRPCBackend());
        }
        else
        {
#ifdef EMBED_ENGINE
            auto confVec = program.get<std::vector<std::string>>("--");
            PRECONDITION(confVec.size() < INT_MAX - 1, "Too many worker configuration options passed through, maximum is {}", INT_MAX);

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

            queryManager = std::make_shared<NES::QueryManager>(std::make_unique<NES::EmbeddedWorkerQuerySubmissionBackend>(
                NES::WorkerConfig{.host = NES::HostAddr(""), .grpc = NES::GrpcAddr("localhost:8080")}, singleNodeWorkerConfig));
#else
            NES_ERROR("No server address given. Please use the -s option to specify the server address or use nebuli-embedded to start a "
                      "single node worker.")
            return 1;
#endif
        }

        NES::SourceStatementHandler sourceStatementHandler{sourceCatalog};
        NES::SinkStatementHandler sinkStatementHandler{sinkCatalog};
        NES::TopologyStatementHandler topologyStatementHandler{queryManager};
        auto queryStatementHandler = std::make_shared<NES::QueryStatementHandler>(queryManager, sourceCatalog, sinkCatalog);
        NES::Repl replClient(
            std::move(sourceStatementHandler),
            std::move(sinkStatementHandler),
            std::move(topologyStatementHandler),
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
    CPPTRACE_CATCH(...)
    {
        NES::tryLogCurrentException();
        return NES::getCurrentErrorCode();
    }
}
