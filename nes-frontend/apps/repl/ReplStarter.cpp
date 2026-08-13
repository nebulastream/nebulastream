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
#include <filesystem>
#include <functional>
#include <iostream>
#include <memory>
#include <optional>
#include <ostream>
#include <ranges>
#include <sstream>
#include <stop_token>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>
#include <unistd.h>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigParsing.hpp>
#include <Configurations/Util.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Identifiers/QualifiedIdentifier.hpp>
#include <Plugins/BuiltinPlugins.hpp>
#include <QueryManager/GRPCQuerySubmissionBackend.hpp>
#include <QueryManager/QueryManager.hpp>
#include <SQLQueryParser/AntlrSQLQueryParser.hpp>
#include <SQLQueryParser/StatementBinder.hpp>
#include <Schema/Schema.hpp>
#include <Sinks/SinkCatalog.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Statements/StatementHandler.hpp>
#include <Statements/StatementJsonSerializers.hpp>
#include <Statements/StatementOutputAssembler.hpp>
#include <Statements/TextOutputFormatter.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/Pointers.hpp>
#include <Util/Signal.hpp>
#include <Util/Strings.hpp>
#include <argparse/argparse.hpp>
#include <cpptrace/from_current.hpp>
#include <cpptrace/utils.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <rfl/json/write.hpp>
#include <ErrorHandling.hpp>
#include <ModelCatalog.hpp>
#include <QueryOptimizer.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <Repl.hpp>
#include <Thread.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerOptimizerConfig.hpp>

#ifdef EMBED_ENGINE
    #include <QueryManager/EmbeddedWorkerQuerySubmissionBackend.hpp>
    #include <SingleNodeWorkerConfiguration.hpp>
    #include <WorkerConfig.hpp>
#endif

/// If repl is executed with an embedded worker, this switch prevents actual port allocation and routes all inter-worker communication
/// via an in-memory channel.

extern void enable_memcom();

namespace
{
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
            return os << rfl::json::write(NES::rowsToJsonArray(result, NES::ReflectionContext{})) << '\n';
    }
    std::unreachable();
}
}

int main(int argc, char** argv)
{
    CPPTRACE_TRY
    {
        NES::setupSignalHandlers();
        NES::loadBuiltinPlugins();
        bool interactiveMode
            = static_cast<int>(cpptrace::isatty(STDIN_FILENO)) != 0 and static_cast<int>(cpptrace::isatty(STDOUT_FILENO)) != 0;

        NES::Thread::initializeThread(NES::Host("nes-repl"), "main");
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
        /// Worker/optimizer config in one schema: the frontend optimizer config
        /// (optimizer.*) and, with the embedded engine, the worker subtree.
        program.add_argument("-w", "--workerConfig")
            .help("worker/optimizer config file (.yaml) with fully qualified keys (worker.*, optimizer.*, ...); the lowest-priority config "
                  "layer below the `--` arguments; conflicting values are an error");
        program.add_argument("--")
            .help("worker/optimizer config arguments, e.g., `-- --optimizer.join_strategy=HASH_JOIN "
                  "--worker.query_engine.number_of_worker_threads=10` (worker options require the embedded engine)")
            .default_value(std::vector<std::string>{})
            .remaining();
        {
            std::ostringstream workerOptimizerConfigHelp;
            NES::generateHelp(workerOptimizerConfigHelp, NES::WorkerOptimizerConfig::getConfigSchema());
            program.add_epilog("worker/optimizer config options (pass after --):\n" + workerOptimizerConfigHelp.str());
        }

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

        /// Parse the `--` config args once and resolve the whole worker/optimizer config; fully qualified
        /// names keep the config roots disjoint (see WorkerOptimizerConfig).
        auto workerOptimizerLiterals = NES::parseCommandLineConfig(program.get<std::vector<std::string>>("--"));
        if (program.is_used("-w"))
        {
            /// The file and the `--` arguments are both run configuration: they must be
            /// disjoint, regardless of whether the values agree.
            try
            {
                auto merged = NES::mergeConfigLayers(
                    {NES::ConfigLayer{
                         .name = "config file", .literals = NES::flattenYAMLConfig(std::filesystem::path{program.get<std::string>("-w")})},
                     NES::ConfigLayer{.name = "command line", .literals = std::move(workerOptimizerLiterals)}});
                if (!merged.overwrites.empty())
                {
                    throw NES::InvalidConfigParameter(
                        "The config file and the command line options must not both set the same option, but both set: {}",
                        fmt::join(
                            merged.overwrites
                                | std::views::transform([](const auto& overwrite)
                                                        { return NES::toLowerCase(fmt::format("{}", overwrite.name)); }),
                            ", "));
                }
                workerOptimizerLiterals = std::move(merged.literals);
            }
            catch (const NES::Exception& e)
            {
                std::cerr << e.what() << '\n';
                return 1;
            }
        }
        auto workerOptimizerConfig = NES::resolveConfiguration<NES::WorkerOptimizerConfig>(workerOptimizerLiterals);
        if (!workerOptimizerConfig.has_value())
        {
            throw NES::InvalidConfigParameter("{}", workerOptimizerConfig.error());
        }
        const auto queryOptimizerConfig = workerOptimizerConfig->queryOptimizer;


        auto sourceCatalogHandle = NES::SourceCatalog::create();
        auto sourceCatalog = NES::copyPtr(sourceCatalogHandle);
        auto sinkCatalog = std::make_shared<NES::SinkCatalog>();
        auto workerCatalog = std::make_shared<NES::WorkerCatalog>();
        auto modelCatalog = std::make_shared<NES::ModelCatalog>();
        std::shared_ptr<NES::QueryManager> queryManager{};
        auto binder = NES::StatementBinder{
            {},
            {},
            sourceCatalog,
            [](auto&& pH1)
            { return NES::AntlrSQLQueryParser::QueryBinder{{}, {}}.bindLogicalQueryPlan(std::forward<decltype(pH1)>(pH1)); }};

#ifdef EMBED_ENGINE
        enable_memcom();
        /// The embedded backend receives the literal schema so it can merge it with per-worker
        /// topology config (CLI wins); the resolved worker root serves the grpc/data reads below.
        const auto& singleNodeWorkerConfig = workerOptimizerConfig->worker;

        /// Derive a routable Host from the gRPC bind address.
        /// The default bind address [::]:8080 is a wildcard, so we use localhost:<port> instead.
        const auto grpcBind = singleNodeWorkerConfig.grpcAddressUri;
        const auto grpcAddr = "localhost" + grpcBind.substr(grpcBind.rfind(':'));
        const auto dataAddr = singleNodeWorkerConfig.dataAddress;
        const NES::WorkerConfig workerConfig{
            .host = NES::Host(grpcAddr),
            .dataAddress = dataAddr,
            .maxOperators = NES::Capacity(NES::CapacityKind::Unlimited{}),
            .downstream = {},
            .config = {}, /// the CLI worker/optimizer literals reach every embedded worker via the resolver below
        };
        workerCatalog->addWorker(workerConfig.host, workerConfig.dataAddress, workerConfig.maxOperators, workerConfig.downstream);
        /// Embedded workers resolve their config from two layers (lowest priority first): the CLI
        /// worker/optimizer literals, then the per-worker registration config (topology wins). Conflicting
        /// values are an error in the repl — there is no override switch here.
        auto resolveWorkerConfiguration = [workerOptimizerLiterals](const NES::WorkerConfig& worker)
        {
            auto [literals, overwrites] = NES::mergeConfigLayers(
                {NES::ConfigLayer{.name = "command line", .literals = workerOptimizerLiterals},
                 NES::ConfigLayer{.name = "worker registration", .literals = worker.config}});
            if (!overwrites.empty())
            {
                throw NES::InvalidConfigParameter(
                    "Conflicting configuration values for worker {}: {}",
                    worker.host.getRawValue(),
                    fmt::join(
                        overwrites
                            | std::views::transform(
                                [](const NES::ConfigOverwrite& overwrite)
                                {
                                    return fmt::format(
                                        "{} ({} vs. {})", overwrite.name, overwrite.overwrittenValue, overwrite.appliedValue);
                                }),
                        ", "));
            }
            auto resolved = NES::resolveConfiguration<NES::WorkerOptimizerConfig>(literals);
            if (!resolved)
            {
                throw NES::InvalidConfigParameter("{}", resolved.error());
            }
            NES_INFO(
                "Configuration of embedded worker {}:\n{}",
                worker.host.getRawValue(),
                NES::formatEffectiveConfig(literals, NES::WorkerOptimizerConfig::getConfigSchema()));
            return std::move(resolved)->worker;
        };
        queryManager
            = std::make_shared<NES::QueryManager>(workerCatalog, NES::createEmbeddedBackend(std::move(resolveWorkerConfiguration)));
        NES::SourceStatementHandler sourceStatementHandler{sourceCatalog};
        NES::SinkStatementHandler sinkStatementHandler{sinkCatalog, NES::DefaultHost(grpcAddr)};
#else
        queryManager = std::make_shared<NES::QueryManager>(workerCatalog, NES::createGRPCBackend());
        NES::SourceStatementHandler sourceStatementHandler{sourceCatalog};
        NES::SinkStatementHandler sinkStatementHandler{sinkCatalog, NES::RequireHostConfig{}};
#endif
        NES::TopologyStatementHandler topologyStatementHandler{queryManager, workerCatalog};
        NES::ModelStatementHandler modelStatementHandler{modelCatalog};
        auto queryOptimizer
            = std::make_shared<NES::QueryOptimizer>(queryOptimizerConfig, sourceCatalog, sinkCatalog, workerCatalog, modelCatalog);
        auto queryStatementHandler = std::make_shared<NES::QueryStatementHandler>(queryManager, queryOptimizer);
        NES::Repl replClient(
            std::move(sourceStatementHandler),
            std::move(sinkStatementHandler),
            std::move(topologyStatementHandler),
            std::move(modelStatementHandler),
            queryStatementHandler,
            std::move(binder),
            errorBehaviour,
            defaultOutputFormat,
            interactiveMode,
            SignalHandler::terminationToken());
        replClient.run();

        bool hasError = false;
        const auto exitStopToken = SignalHandler::terminationToken();
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
                while (!queryManager->getRunningQueries().empty() && !exitStopToken.stop_requested())
                {
                    NES_DEBUG("Waiting for termination")
                    std::this_thread::sleep_for(std::chrono::milliseconds(50));
                }
                if (exitStopToken.stop_requested())
                {
                    NES_WARNING("Termination signal received; aborting on-exit wait");
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
