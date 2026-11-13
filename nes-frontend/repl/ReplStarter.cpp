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
#include <csignal>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <iostream>
#include <iterator>
#include <optional>
#include <ranges>
#include <sstream>
#include <stop_token>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include <unistd.h>

#include <Configurations/ConfigField.hpp>
#include <Configurations/ConfigLiteral.hpp>
#include <Configurations/ConfigParsing.hpp>
#include <Configurations/ConfigResolution.hpp>
#include <Configurations/Util.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Plugins/BuiltinPlugins.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/Signal.hpp>
#include <Util/Strings.hpp>
#include <argparse/argparse.hpp>
#include <cpptrace/from_current.hpp>
#include <cpptrace/utils.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <nes-coordinator-bridge/coordinator.h>
#include <rust/cxx.h>
#include <BridgeConfig.hpp>
#include <BridgeError.hpp>
#include <ErrorHandling.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <Repl.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <Thread.hpp>
#include <Version.hpp>

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
    /// NOLINTNEXTLINE(bugprone-throwing-static-initialization) std::stop_source only throws on allocation failure
    static inline std::stop_source signalSource;

public:
    static void setup()
    {
        if (const auto previousHandler = std::signal(SIGTERM, [](int) { [[maybe_unused]] auto dontCare = signalSource.request_stop(); });
            previousHandler == SIG_ERR)
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
}

int main(const int argc, char** argv)
{
    if (NES::hasVersionFlag(argc, argv))
    {
#ifdef EMBED_ENGINE
        NES::printVersion("nes-repl-embedded");
#else
        NES::printVersion("nes-repl");
#endif
        return 0;
    }
    CPPTRACE_TRY
    {
        NES::setupSignalHandlers();
        NES::loadBuiltinPlugins();
        const bool interactiveMode
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
            .help(
                fmt::format(
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
        program.add_argument("--db")
            .default_value(std::string{})
            .help("path to a persistent sqlite catalog; empty (the default) uses an ephemeral in-memory catalog");
#ifdef EMBED_ENGINE
        const auto declaredConfig = NES::createConfigSchema(
            NES::SingleNodeWorkerConfiguration::getConfigSchema(), NES::QueryOptimizerConfiguration::getConfigSchema());
#else
        const auto declaredConfig = NES::QueryOptimizerConfiguration::getConfigSchema();
#endif
        /// Worker/optimizer config in one schema: the frontend optimizer config
        /// (optimizer.*) and, with the embedded engine, the worker subtree.
        program.add_argument("-w", "--workerConfig")
            .help(
                "worker/optimizer config file (.yaml) with fully qualified keys (worker.*, optimizer.*, ...); must be disjoint from "
                "the config options after `--`");
        program.add_argument("--")
            .help(
                "worker/optimizer config arguments, e.g., `-- --optimizer.join_strategy=HASH_JOIN "
                "--worker.query_engine.number_of_worker_threads=10` (worker options require the embedded engine)")
            .default_value(std::vector<std::string>{})
            .remaining();
        {
            std::ostringstream workerOptimizerConfigHelp;
            NES::generateHelp(workerOptimizerConfigHelp, declaredConfig);
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

        /// Output format is fixed for the whole session. argparse defaults -f to TEXT and
        /// restricts it to TEXT or JSON, so JSON output happens only when explicitly requested.
        const bool jsonOutput = program.get<std::string>("-f") == "JSON";


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

        auto workerOptimizerLiterals = NES::parseCommandLineConfig(program.get<std::vector<std::string>>("--"));
        if (program.is_used("-w"))
        {
            /// The file and the `--` arguments are both run configuration: they must be
            /// disjoint, regardless of whether the values agree.
            try
            {
                auto [literals, overwrites] = NES::mergeConfigLayers(
                    {NES::ConfigLayer{
                         .name = "config file", .literals = NES::flattenYAMLConfig(std::filesystem::path{program.get<std::string>("-w")})},
                     NES::ConfigLayer{.name = "command line", .literals = std::move(workerOptimizerLiterals)}});
                if (!overwrites.empty())
                {
                    throw NES::InvalidConfigParameter(
                        "The config file and the command line options must not both set the same option, but both set: {}",
                        fmt::join(
                            overwrites
                                | std::views::transform([](const auto& overwrite)
                                                        { return NES::toLowerCase(fmt::format("{}", overwrite.name)); }),
                            ", "));
                }
                workerOptimizerLiterals = std::move(literals);
            }
            catch (const NES::Exception& e)
            {
                std::cerr << e.what() << '\n';
                return 1;
            }
        }
        if (const auto resolvedConfig = NES::toExpected(NES::resolveConfigFullyQualified(workerOptimizerLiterals, declaredConfig));
            !resolvedConfig.has_value())
        {
            throw NES::InvalidConfigParameter("{}", resolvedConfig.error());
        }

        const auto [optimizerLiterals, workerLiterals] = NES::Bridge::splitOptimizerConfig(workerOptimizerLiterals);
        const auto optimizerConfigJson = NES::Bridge::literalsToJson(optimizerLiterals);


#ifdef EMBED_ENGINE
        constexpr auto workerMode = NES::Bridge::WorkerMode::Embedded;
#else
        constexpr auto workerMode = NES::Bridge::WorkerMode::Remote;
#endif
        /// An empty --db uses an ephemeral in-memory catalog; a path uses a persistent sqlite one.
        const auto dbPath = program.get<std::string>("--db");
        const auto backend = dbPath.empty() ? NES::Bridge::StateBackendKind::Memory : NES::Bridge::StateBackendKind::Sqlite;
        auto coordinator = NES::Bridge::start_coordinator(
            backend,
            rust::Str{dbPath.data(), dbPath.size()},
            workerMode,
            rust::Str{optimizerConfigJson.data(), optimizerConfigJson.size()});

#ifdef EMBED_ENGINE
        /// The coordinator can only place fragments on a worker that the catalog lists.
        /// The embedded worker is registered at the default host, so a statement without a HOST clause is placed on it.
        std::vector<std::string> embeddedWorkerOptions{"'localhost:9090' AS DATA"};
        std::ranges::transform(workerLiterals, std::back_inserter(embeddedWorkerOptions), NES::Bridge::configOption);
        const auto embeddedWorkerStatement
            = fmt::format("CREATE WORKER '{}' SET ({})", std::string{coordinator->default_host()}, fmt::join(embeddedWorkerOptions, ", "));
        NES::Bridge::throwIfError(
            coordinator->submit(rust::Str{embeddedWorkerStatement}, NES::Bridge::WaitMode::None, 0, jsonOutput).error);
#endif

        const NES::Repl replClient(*coordinator, errorBehaviour, jsonOutput, interactiveMode, SignalHandler::terminationToken());
        replClient.run();

        bool hasError = false;
        /// NOLINTNEXTLINE(bugprone-unchecked-optional-access) validated by argparse .choices()
        switch (magic_enum::enum_cast<OnExitBehavior>(program.get<std::string>("--on-exit")).value())
        {
            case OnExitBehavior::STOP_QUERIES: {
                /// A DROP without a filter applies to every installed query.
                /// The controller runs in this process, so exiting now would leave the queries running on their workers.
                if (const auto dropped = coordinator->submit(rust::Str{"DROP QUERY"}, NES::Bridge::WaitMode::None, 0, jsonOutput);
                    not NES::Bridge::isOk(dropped.error))
                {
                    NES_ERROR("Could not stop the queries: {}", std::string{dropped.error.msg});
                    hasError = true;
                }
                [[fallthrough]];
            }
            case OnExitBehavior::WAIT_FOR_QUERY_TERMINATION: {
                const auto exitToken = SignalHandler::terminationToken();
                /// The wait below blocks this thread, so a second thread checks for a termination signal and cancels the wait.
                std::jthread watcher{[&](const std::stop_token& waitEnded)
                                     {
                                         while (not waitEnded.stop_requested() and not exitToken.stop_requested())
                                         {
                                             constexpr std::chrono::milliseconds pollInterval{50};
                                             std::this_thread::sleep_for(pollInterval);
                                         }
                                         if (exitToken.stop_requested())
                                         {
                                             coordinator->cancel_waits();
                                         }
                                     }};
                coordinator->await_queries();
                watcher.request_stop();

                if (exitToken.stop_requested())
                {
                    NES_WARNING("Termination signal received; aborting on-exit wait");
                }
                break;
            }
            case OnExitBehavior::DO_NOTHING:
                break;
        }
        return hasError ? 1 : 0;
    }
    CPPTRACE_CATCH(...)
    {
        NES::tryLogCurrentException();
        return NES::getCurrentErrorCode();
    }
}
