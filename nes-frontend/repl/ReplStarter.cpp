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

#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <exception>
#include <iostream>
#include <mutex>
#include <optional>
#include <ranges>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <unistd.h>

#include <Identifiers/Identifiers.hpp>
#include <Plugins/BuiltinPlugins.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/Signal.hpp>
#include <argparse/argparse.hpp>
#include <cpptrace/from_current.hpp>
#include <cpptrace/utils.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <magic_enum/magic_enum.hpp>
#include <nes-coordinator-bridge/coordinator.h>
#include <rfl/json/write.hpp>
#include <rust/cxx.h>
#include <BridgeError.hpp>
#include <ErrorHandling.hpp>
#include <QueryOptimizerConfiguration.hpp>
#include <Repl.hpp>
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
        program.add_argument("--coordinator")
            .help("base URL of a running nes-server, used instead of a coordinator in this process, e.g. http://127.0.0.1:8081");

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
        /// query optimizer config
        program.add_argument("--optimizer")
            .default_value<std::vector<std::string>>({})
            .append()
            .help("changes optimizer default values. e.g. join_strategy=HASH_JOIN");
        program.add_argument("--db")
            .default_value(std::string{})
            .help("path to a persistent sqlite catalog; empty (the default) uses an ephemeral in-memory catalog");

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

        /// The --optimizer key=value pairs are validated here, before the coordinator starts, so an unknown key fails early.
        /// They are serialized to the flat JSON object that the coordinator passes to the planner.
        /// An empty config leaves the planner defaults in place.
        std::string optimizerConfigJson;
        if (program.is_used("--optimizer"))
        {
            std::unordered_map<std::string, std::string> optimizerRawConfig;
            for (const auto& optimizerConfigString : program.get<std::vector<std::string>>("--optimizer"))
            {
                const auto pos = optimizerConfigString.find('=');
                if (pos == std::string::npos)
                {
                    NES_ERROR("Invalid optimizer argument. Requires argument like 'CONFIG=VALUE' but got '{}'", optimizerConfigString)
                    return 1;
                }
                optimizerRawConfig[optimizerConfigString.substr(0, pos)] = optimizerConfigString.substr(pos + 1);
            }
            NES::QueryOptimizerConfiguration{}.overwriteConfigWithCommandLineInput(optimizerRawConfig);
            optimizerConfigJson = rfl::json::write(optimizerRawConfig);
        }


        const bool remoteCoordinator = program.is_used("--coordinator");
        if (remoteCoordinator and (program.is_used("--db") or program.is_used("--optimizer")))
        {
            std::cerr << "--db and --optimizer configure a coordinator in this process; they cannot be combined with --coordinator\n";
            return 1;
        }

#ifdef EMBED_ENGINE
        constexpr auto workerMode = NES::Bridge::WorkerMode::Embedded;
#else
        constexpr auto workerMode = NES::Bridge::WorkerMode::Remote;
#endif
        /// An empty --db uses an ephemeral in-memory catalog; a path uses a persistent sqlite one.
        const auto dbPath = program.get<std::string>("--db");
        auto coordinator = [&]
        {
            if (remoteCoordinator)
            {
                const auto url = program.get<std::string>("--coordinator");
                return NES::Bridge::connect_coordinator(rust::Str{url.data(), url.size()});
            }
            return NES::Bridge::start_coordinator(
                rust::Str{dbPath.data(), dbPath.size()}, workerMode, rust::Str{optimizerConfigJson.data(), optimizerConfigJson.size()});
        }();

#ifdef EMBED_ENGINE
        /// The coordinator can only place fragments on a worker that the catalog lists.
        /// The embedded worker is registered at the default host, so a statement without a HOST clause is placed on it.
        /// A coordinator elsewhere brings its own workers, so nothing is registered for it here.
        if (not remoteCoordinator)
        {
            const auto embeddedWorkerStatement
                = fmt::format("CREATE WORKER '{}' SET ('localhost:9090' AS DATA)", std::string{coordinator->default_host()});
            NES::Bridge::raiseReported(
                coordinator->submit(rust::Str{embeddedWorkerStatement}, NES::Bridge::WaitMode::None, 0, jsonOutput).error);
        }
#endif

        const NES::Repl replClient(*coordinator, errorBehaviour, jsonOutput, interactiveMode, SignalHandler::terminationToken());
        replClient.run();

        /// NOLINTNEXTLINE(bugprone-unchecked-optional-access) validated by argparse .choices()
        switch (magic_enum::enum_cast<OnExitBehavior>(program.get<std::string>("--on-exit")).value())
        {
            case OnExitBehavior::STOP_QUERIES:
                /// A DROP without a filter applies to every installed query.
                /// Nothing waits for them to reach a stopped state; the process exits straight after.
                /// WAIT_FOR_QUERY_TERMINATION is the mode that waits.
                coordinator->submit(rust::Str{"DROP QUERY"}, NES::Bridge::WaitMode::None, 0, jsonOutput);
                break;
            case OnExitBehavior::WAIT_FOR_QUERY_TERMINATION: {
                /// This thread parks inside the coordinator until the queries finish, so it cannot notice a termination signal itself.
                /// A second thread waits on the token and releases the wait.
                /// The cancel runs there rather than in the signal handler,
                /// which may interrupt this thread at any point and must not take a lock that this thread might already hold.
                auto exitToken = SignalHandler::terminationToken();
                std::mutex mutex;
                std::condition_variable_any waitEnded;
                bool ended = false;

                std::thread watcher(
                    [&]
                    {
                        std::unique_lock lock{mutex};
                        if (!waitEnded.wait(lock, exitToken, [&] { return ended; }))
                        {
                            coordinator->cancel_waits();
                        }
                    });

                coordinator->submit(rust::Str{"SHOW QUERIES"}, NES::Bridge::WaitMode::UntilTerminated, 0, jsonOutput);
                {
                    const std::lock_guard lock{mutex};
                    ended = true;
                }
                waitEnded.notify_all();
                watcher.join();

                if (exitToken.stop_requested())
                {
                    NES_WARNING("Termination signal received; aborting on-exit wait");
                }
                break;
            }
            case OnExitBehavior::DO_NOTHING:
                break;
        }
        return 0;
    }
    CPPTRACE_CATCH(...)
    {
        NES::tryLogCurrentException();
        return NES::getCurrentErrorCode();
    }
}
