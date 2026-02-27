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
#include <csignal>
#include <cstdint>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <ostream>
#include <ranges>
#include <stop_token>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>
#include <unistd.h>

#include <Identifiers/Identifiers.hpp>
#include <QueryManager/GRPCQuerySubmissionBackend.hpp>
#include <QueryManager/QueryManager.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Statements/JsonOutputFormatter.hpp>
#include <Statements/StatementHandler.hpp>
#include <Statements/StatementOutputAssembler.hpp>
#include <Statements/TextOutputFormatter.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/Pointers.hpp>
#include <Util/Signal.hpp>
#include <argparse/argparse.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <nlohmann/json.hpp>
#include <ErrorHandling.hpp>
#include <LegacyOptimizer.hpp>
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
NES::UniquePtr<NES::GRPCQuerySubmissionBackend> createGRPCBackend(std::string grpcAddr)
{
    return std::make_unique<NES::GRPCQuerySubmissionBackend>(NES::WorkerConfig{.grpc = NES::GrpcAddr(std::move(grpcAddr)), .config = {}});
}

enum class OnExitBehavior : uint8_t
{
    WAIT_FOR_QUERY_TERMINATION,
    STOP_QUERIES,
    DO_NOTHING,
};

class SignalHandler
{
    static inline std::stop_source signalSource;

public:
    static void setup()
    {
        const auto previousHandler = std::signal(SIGTERM, [](int) { [[maybe_unused]] auto dontCare = signalSource.request_stop(); });
        if (previousHandler == SIG_ERR)
        {
            NES_WARNING("Could not install signal handler for SIGTERM. Repl might not respond to termination signals.");
        }
        else
        {
            INVARIANT(
                previousHandler == nullptr,
                "The SignalHandler does not restore the pre existing signal handler and thus it expects no handler to exist");
        }
    }

    static std::stop_token terminationToken() { return signalSource.get_token(); }
};

std::ostream& printStatementResult(std::ostream& os, NES::StatementOutputFormat format, const auto& statement)
{
    NES::StatementOutputAssembler<std::remove_cvref_t<decltype(statement)>> assembler{};
    auto result = assembler.convert(statement);
    switch (format)
    {
        case NES::StatementOutputFormat::TEXT:
            return os << toText(result);
        case NES::StatementOutputFormat::JSON:
            return os << nlohmann::json(result).dump() << '\n';
    }
    std::unreachable();
}
}

int main(int argc, char** argv)
{
    CPPTRACE_TRY
    {
        NES::setupSignalHandlers();
        bool interactiveMode
            = static_cast<int>(cpptrace::isatty(STDIN_FILENO)) != 0 and static_cast<int>(cpptrace::isatty(STDOUT_FILENO)) != 0;

        NES::Thread::initializeThread(NES::WorkerId("nes-repl"), "main");
        NES::Logger::setupLogging("nes-repl.log", NES::LogLevel::LOG_ERROR, false);
        SignalHandler::setup();

        using argparse::ArgumentParser;
        ArgumentParser program("nes-repl");
        program.add_argument("-d", "--debug").flag().help("Dump the query plan and enable debug logging");
        program.add_argument("-s", "--server").help("Server URI to connect to").default_value(std::string{"localhost:8080"});

        program.add_argument("--on-exit")
            .choices(
                magic_enum::enum_name(OnExitBehavior::WAIT_FOR_QUERY_TERMINATION),
                magic_enum::enum_name(OnExitBehavior::STOP_QUERIES),
                magic_enum::enum_name(OnExitBehavior::DO_NOTHING))
            .default_value(std::string(magic_enum::enum_name(OnExitBehavior::DO_NOTHING)))
            .help(fmt::format(
                "on exit behavior: [{}]",
                fmt::join(
                    std::views::transform(
                        magic_enum::enum_values<OnExitBehavior>(),
                        [](const auto& exitBehavior) { return magic_enum::enum_name(exitBehavior); }),
                    ", ")));

        program.add_argument("-e", "--error-behaviour")
            .choices("FAIL_FAST", "RECOVER", "CONTINUE_AND_FAIL")
            .help(
                "Fail and return non-zero exit code on first error, ignore error and continue, or continue and return non-zero exit code");
        program.add_argument("-f").default_value("TEXT").choices("TEXT", "JSON").help("Output format");

#ifdef EMBED_ENGINE
        /// single node worker config
        program.add_argument("--")
            .help("arguments passed to the worker config, e.g., `-- --worker.query_engine.number_of_worker_threads=10`")
            .default_value(std::vector<std::string>{})
            .remaining();
#endif

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
                return magic_enum::enum_cast<NES::ErrorBehaviour>(program.get<std::string>("-e")).value();
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
            queryManager = std::make_shared<NES::QueryManager>(createGRPCBackend(program.get<std::string>("-s")));
        }
        else
        {
#ifdef EMBED_ENGINE
            auto confVec = program.get<std::vector<std::string>>("--");
            const int singleNodeArgC = static_cast<int>(confVec.size() + 1);
            std::vector<const char*> singleNodeArgV;
            singleNodeArgV.reserve(singleNodeArgC + 1);
            singleNodeArgV.push_back("nes-single-node-worker"); /// dummy option as arg expects first arg to be the program name
            for (auto& arg : confVec)
            {
                singleNodeArgV.push_back(arg.c_str());
            }
            auto singleNodeWorkerConfig = NES::loadConfiguration<NES::SingleNodeWorkerConfiguration>(singleNodeArgC, singleNodeArgV.data())
                                              .value_or(NES::SingleNodeWorkerConfiguration{});

            queryManager = std::make_shared<NES::QueryManager>(std::make_unique<NES::EmbeddedWorkerQuerySubmissionBackend>(
                NES::WorkerConfig{.grpc = NES::GrpcAddr("localhost:8080"), .config = {}}, singleNodeWorkerConfig));
#else
            NES_ERROR("No server address given. Please use the -s option to specify the server address or use nes-repl-embedded to start a "
                      "single node worker.")
            return 1;
#endif
        }

        NES::SourceStatementHandler sourceStatementHandler{sourceCatalog};
        NES::SinkStatementHandler sinkStatementHandler{sinkCatalog};
        NES::TopologyStatementHandler topologyStatementHandler{queryManager};
        auto optimizer = std::make_shared<NES::LegacyOptimizer>(sourceCatalog, sinkCatalog);
        auto queryStatementHandler = std::make_shared<NES::QueryStatementHandler>(queryManager, optimizer);
        NES::Repl replClient(
            std::move(sourceStatementHandler),
            std::move(sinkStatementHandler),
            std::move(topologyStatementHandler),
            queryStatementHandler,
            std::move(binder),
            errorBehaviour,
            defaultOutputFormat,
            interactiveMode,
            SignalHandler::terminationToken());
        replClient.run();

        bool hasError = false;
        /// NOLINTNEXTLINE(bugprone-unchecked-optional-access) validated by argparse .choices()
        switch (magic_enum::enum_cast<OnExitBehavior>(program.get<std::string>("--on-exit")).value())
        {
            case OnExitBehavior::STOP_QUERIES:
                for (auto& query : queryManager->getRunningQueries())
                {
                    auto result = queryStatementHandler->operator()(NES::DropQueryStatement{.id = query});
                    const NES::StatementOutputAssembler<NES::DropQueryStatementResult> assembler{};
                    if (!result.has_value())
                    {
                        NES_ERROR("Could not stop query: {}", result.error().what());
                        hasError = true;
                        continue;
                    }
                    /// NOLINTNEXTLINE(bugprone-unchecked-optional-access) validated by argparse .choices()
                    printStatementResult(
                        std::cout, magic_enum::enum_cast<NES::StatementOutputFormat>(program.get("-f")).value(), result.value());
                }
                [[clang::fallthrough]];
            case OnExitBehavior::WAIT_FOR_QUERY_TERMINATION:
                while (!queryManager->getRunningQueries().empty())
                {
                    NES_DEBUG("Waiting for termination")
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                }
                break;
            case OnExitBehavior::DO_NOTHING:
                break;
        }

        if (hasError)
        {
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
