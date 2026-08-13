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
#include <iostream>
#include <semaphore>
#include <sstream>
#include <string>
#include <vector>
#include <Configurations/Util.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Plugins/PluginCatalog.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/Signal.hpp>
#include <argparse/argparse.hpp>
#include <cpptrace/from_current.hpp>
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
            .help("worker config file (.yaml); options given after `--` override values from the file");
        program.add_argument("--")
            .help("worker config options, e.g. `-- --grpc=[::]:8080 --worker.query_engine.number_of_worker_threads=4`")
            .default_value(std::vector<std::string>{})
            .remaining();
        {
            std::ostringstream configOptionsHelp;
            NES::generateHelp<NES::SingleNodeWorkerConfiguration>(configOptionsHelp);
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

        /// Re-assemble an argv for the option parser from the config file given via `-w` and the
        /// options captured after `--`.
        /// NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic) argv[0] is the program name from main's argv
        std::vector<std::string> configArgs{argv[0]};
        if (program.is_used("-w"))
        {
            configArgs.push_back("--configPath=" + program.get<std::string>("-w"));
        }
        const auto remainingArgs = program.get<std::vector<std::string>>("--");
        configArgs.insert(configArgs.end(), remainingArgs.begin(), remainingArgs.end());
        std::vector<const char*> configArgv;
        configArgv.reserve(configArgs.size());
        for (const auto& arg : configArgs)
        {
            configArgv.push_back(arg.c_str());
        }

        auto configuration
            = NES::loadConfiguration<NES::SingleNodeWorkerConfiguration>(static_cast<int>(configArgv.size()), configArgv.data());
        if (!configuration)
        {
            return 0;
        }
        {
            NES::Thread::initializeThread(NES::Host(configuration->dataAddress.getValue()), "main");
            NES::GRPCServer workerService{NES::SingleNodeWorker(*configuration, NES::Host(configuration->dataAddress.getValue()))};

            grpc::ServerBuilder builder;
            builder.SetMaxMessageSize(-1);
            builder.AddListeningPort(configuration->grpcAddressUri.getValue(), grpc::InsecureServerCredentials());
            builder.RegisterService(&workerService);
            grpc::EnableDefaultHealthCheckService(true);

            const auto server = builder.BuildAndStart();
            if (!server)
            {
                NES_ERROR("Failed to start GRPC Server. Stopping worker...");
                return 1;
            }

            const auto hook = shutdownHook(*server);
            NES_INFO("Server listening on {}", configuration->grpcAddressUri.getValue());
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
