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

#include <cstdlib>
#include <exception>
#include <filesystem>
#include <iostream>
#include <optional>
#include <ranges>
#include <sstream>
#include <stop_token>
#include <string>
#include <utility>
#include <vector>
#include <pthread.h>
/// The POSIX signal APIs used below are not provided by the C++ <csignal> header.
/// NOLINTNEXTLINE(modernize-deprecated-headers)
#include <signal.h>
#include <Configurations/ConfigParsing.hpp>
#include <Configurations/Util.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Plugins/BuiltinPlugins.hpp>
#include <Util/Files.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/Signal.hpp>
#include <Util/Strings.hpp>
#include <argparse/argparse.hpp>
#include <cpptrace/from_current.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <folly/Synchronized.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>
#include <ErrorHandling.hpp>
#include <GrpcService.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <Thread.hpp>
#include <Version.hpp>
#include <scope_guard.hpp>

namespace
{

class Cleanup
{
    static constexpr int SIGNAL_EXIT_CODE_OFFSET = 128;

    struct Internal
    {
        std::optional<int> terminationSignal;
        grpc::Server* server = nullptr;
    };

    folly::Synchronized<Internal> cleanupMutex;

public:
    std::optional<int> clearServerAndGetTerminationSignal()
    {
        auto cleanup = cleanupMutex.wlock();
        cleanup->server = nullptr;
        auto result = cleanup->terminationSignal;
        cleanup->terminationSignal.reset();
        return result;
    }

    std::optional<int> setServerIfNotTerminated(grpc::Server* server)
    {
        auto cleanup = cleanupMutex.wlock();
        if (cleanup->terminationSignal)
        {
            return cleanup->terminationSignal;
        }
        cleanup->server = server;
        return std::nullopt;
    }

    void requestTermination(int signal)
    {
        auto cleanup = cleanupMutex.wlock();
        cleanup->terminationSignal = signal + SIGNAL_EXIT_CODE_OFFSET;
        if (cleanup->server != nullptr)
        {
            cleanup->server->Shutdown();
        }
    }
};

/// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
Cleanup cleanup;

/// Block termination signals before any worker threads are created. All subsequently created threads inherit this mask, allowing
/// a dedicated thread to synchronously wait for the signals without doing non-async-signal-safe work in a signal handler.
/// sigset_t is provided by <signal.h>, but clang-tidy's include-cleaner does not recognize the indirect platform typedef.
/// NOLINTNEXTLINE(misc-include-cleaner)
bool blockTerminationSignals(sigset_t& terminationSignals)
{
    sigemptyset(&terminationSignals);
    sigaddset(&terminationSignals, SIGINT);
    sigaddset(&terminationSignals, SIGTERM);
    return pthread_sigmask(SIG_BLOCK, &terminationSignals, nullptr) == 0;
}

/// Waits from the start of the process and can terminate even if the GRPC server is not running yet
NES::Thread terminationThread(const sigset_t terminationSignals)
{
    return {
        "shutdown-hook",
        [terminationSignals](const std::stop_token& stopToken) mutable
        {
            /// Wake sigwait with a thread-directed signal when the thread is asked to stop during a clean shutdown.
            const auto waitingThread = pthread_self();
            const std::stop_callback stopCallback(
                stopToken,
                [waitingThread]
                {
                    if (const auto error = pthread_kill(waitingThread, SIGINT); error != 0)
                    {
                        NES_ERROR("Failed to wake the termination thread: {} ({})", NES::getErrorMessage(error), error);
                    }
                });

            int signal{};
            const auto error = sigwait(&terminationSignals, &signal);
            if (error != 0)
            {
                NES_ERROR("Failed to wait for a termination signal: {} ({})", NES::getErrorMessage(error), error);
                return;
            }
            if (stopToken.stop_requested())
            {
                return;
            }
            NES_INFO("Received signal {}. Shutting down.", signal);
            cleanup.requestTermination(signal);
        }};
}
}

int main(const int argc, const char* argv[])
{
    if (NES::hasVersionFlag(argc, argv))
    {
        NES::printVersion(NES::SingleNodeWorkerBinaryName);
        return 0;
    }
    NES::setupSignalHandlers();
    sigset_t terminationSignals{};
    if (!blockTerminationSignals(terminationSignals))
    {
        return 1;
    }
    NES::Logger::setupLogging("singleNodeWorker.log", NES::LogLevel::LOG_DEBUG);
    SCOPE_EXIT
    {
        if (const auto logger = NES::Logger::getInstance())
        {
            logger->forceFlush();
        }
    };

    /// Register termination handler right now so that the process can be terminated before the rest of the system has started up.
    const auto terminationHandler = terminationThread(terminationSignals);

    CPPTRACE_TRY
    {
        /// Register built-in plugins before any registry lookup.
        NES::loadBuiltinPlugins();

        argparse::ArgumentParser program("nes-single-node-worker");
        program.add_argument("-w", "--workerConfig")
            .help("worker config file (.yaml) with fully qualified keys; must be disjoint from the config options after `--`");
        program.add_argument("--")
            .help("worker config options, e.g. `-- --grpc=[::]:8080 --worker.query_engine.number_of_worker_threads=4`")
            .default_value(std::vector<std::string>{})
            .remaining();
        {
            std::ostringstream configOptionsHelp;
            NES::generateHelp(configOptionsHelp, NES::SingleNodeWorkerConfiguration::getConfigSchema());
            program.add_epilog("worker config options (pass after --):\n" + configOptionsHelp.str());
        }
        try
        {
            program.parse_args(argc, argv);
        }
        catch (const std::exception& e)
        {
            std::cerr << e.what() << '\n' << program;
            return 1;
        }

        auto passedConfig = NES::parseCommandLineConfig(program.get<std::vector<std::string>>("--"));
        if (program.is_used("-w"))
        {
            /// The config file and the `--` arguments are both run configuration: they must be
            /// disjoint, regardless of whether the values agree.
            auto merged = NES::mergeConfigLayers(
                {NES::ConfigLayer{
                     .name = "config file", .literals = NES::flattenYAMLConfig(std::filesystem::path{program.get<std::string>("-w")})},
                 NES::ConfigLayer{.name = "command line", .literals = std::move(passedConfig)}});
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
            passedConfig = std::move(merged.literals);
        }
        auto resolvedConfiguration = NES::resolveConfiguration<NES::SingleNodeWorkerConfiguration>(passedConfig);
        if (!resolvedConfiguration.has_value())
        {
            throw NES::InvalidConfigParameter("{}", resolvedConfiguration.error());
        }
        NES_INFO(
            "Loaded configuration:\n{}", NES::formatEffectiveConfig(passedConfig, NES::SingleNodeWorkerConfiguration::getConfigSchema()));
        const auto configuration = std::move(resolvedConfiguration).value();
        {
            NES::Thread::initializeThread(NES::Host(configuration.dataAddress), "main");
            NES::GRPCServer workerService{NES::SingleNodeWorker(configuration, NES::Host(configuration.dataAddress))};

            grpc::ServerBuilder builder;
            builder.SetMaxMessageSize(-1);
            builder.AddListeningPort(configuration.grpcAddressUri, grpc::InsecureServerCredentials());
            builder.RegisterService(&workerService);
            grpc::EnableDefaultHealthCheckService(true);

            const auto server = builder.BuildAndStart();
            if (!server)
            {
                NES_ERROR("Failed to start GRPC Server. Stopping worker...");
                return 1;
            }

            if (auto terminationSignal = cleanup.setServerIfNotTerminated(server.get()))
            {
                return *terminationSignal;
            }
            NES_INFO("Server listening on {}", configuration.grpcAddressUri);
            server->Wait();
            if (auto terminationSignal = cleanup.clearServerAndGetTerminationSignal())
            {
                return *terminationSignal;
            }
            NES_INFO("GRPC Server was shutdown. Terminating the SingleNodeWorker");
        }
        return 0;
    }
    CPPTRACE_CATCH(...)
    {
        NES::tryLogCurrentException();
        return NES::getCurrentErrorCode();
    }
}
