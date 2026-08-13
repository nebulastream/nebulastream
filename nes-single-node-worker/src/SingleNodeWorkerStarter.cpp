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

/// The POSIX signal APIs used below are not provided by the C++ <csignal> header.
/// NOLINTNEXTLINE(modernize-deprecated-headers)
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <iostream>
#include <ranges>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <signal.h>
#include <Configurations/ConfigParsing.hpp>
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
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>
#include <ErrorHandling.hpp>
#include <GrpcService.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <Thread.hpp>
#include <Version.hpp>

namespace
{
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

NES::Thread shutdownHook(grpc::Server& server, const sigset_t terminationSignals)
{
    return {
        "shutdown-hook",
        [&, terminationSignals]() mutable
        {
            int signal{};
            const auto error = sigwait(&terminationSignals, &signal);
            if (error != 0)
            {
                NES_ERROR("Failed to wait for a termination signal: {}", error)
            }
            else
            {
                NES_INFO("Received signal {}. Shutting down.", signal);
            }
            server.Shutdown();
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
    CPPTRACE_TRY
    {
        NES::setupSignalHandlers();
        sigset_t terminationSignals{};
        if (!blockTerminationSignals(terminationSignals))
        {
            return 1;
        }
        NES::Logger::setupLogging("singleNodeWorker.log", NES::LogLevel::LOG_DEBUG);
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

            const auto hook = shutdownHook(*server, terminationSignals);
            NES_INFO("Server listening on {}", configuration.grpcAddressUri);
            server->Wait();
            NES_INFO("GRPC Server was shutdown. Terminating the SingleNodeWorker");
        }
        NES::Logger::getInstance()->forceFlush();
        return 0;
    }
    CPPTRACE_CATCH(...)
    {
        NES::tryLogCurrentException();
        return NES::getCurrentErrorCode();
    }
}
