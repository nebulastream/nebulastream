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

#include <csignal>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <iostream>
#include <optional>
#include <ranges>
#include <semaphore>
#include <sstream>
#include <string>
#include <vector>
#include <Configurations/ConfigParsing.hpp>
#include <Configurations/Util.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Plugins/PluginCatalog.hpp>
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

namespace
{
/// This logic is related to handling shutdown of the system when a signal is received.
/// GRPC does not like it if it is accessed via the signal handler. Effectively, this creates a thread which waits for
/// the shutdownBarrier to be released by the signal handler and then shuts the grpc server down, which unblocks the `Wait` call
/// in the main function.
/// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables, cert-err58-cpp) - required for signal handler communication
std::binary_semaphore shutdownBarrier{0};

void signalHandler(int signal)
{
    NES_INFO("Received signal {}. Shutting down.", signal);
    shutdownBarrier.release();
}

NES::Thread shutdownHook(grpc::Server& server)
{
    return {
        "shutdown-hook",
        [&]()
        {
            shutdownBarrier.acquire();
            server.Shutdown();
        }};
}
}

int main(const int argc, const char* argv[])
{
    CPPTRACE_TRY
    {
        NES::setupSignalHandlers();
        NES::Logger::setupLogging("singleNodeWorker.log", NES::LogLevel::LOG_DEBUG);
        if (std::signal(SIGINT, signalHandler) == SIG_ERR)
        {
            NES_ERROR("Failed to set SIGINT signal handler")
        }
        if (std::signal(SIGTERM, signalHandler) == SIG_ERR)
        {
            NES_ERROR("Failed to set SIGTERM signal handler")
        }
        /// Register built-in plugins (via the catalog's constructor) and load dynamic plugins
        /// (shared objects listed in NES_PLUGINS) before any registry lookup.
        NES::PluginCatalog pluginCatalog;
#ifdef NES_STATIC_WORKER
        /// Statically linked variant: a plugin .so NEEDs libnes.so, so loading one would drag in
        /// a second copy of every registry and global next to the static components. Reject
        /// instead of silently registering into the wrong copy.
        if (std::getenv("NES_PLUGINS") != nullptr)
        {
            NES_ERROR("NES_PLUGINS is set, but this worker is statically linked and cannot load plugins. "
                      "Unset NES_PLUGINS or use the dynamically linked nes-single-node-worker.");
            return 1;
        }
#else
        pluginCatalog.loadFromEnvironment();
#endif

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

            const auto hook = shutdownHook(*server);
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
