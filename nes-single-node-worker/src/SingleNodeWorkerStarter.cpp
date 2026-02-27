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
#include <semaphore>
#include <thread>
#include <Configurations/Util.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/Signal.hpp>
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
std::binary_semaphore shutdownBarrier{0};

void signalHandler(int signal)
{
    NES_INFO("Received signal {}. Shutting down.", signal);
    shutdownBarrier.release();
}

std::jthread shutdownHook(grpc::Server& server)
{
    return std::jthread(
        [&]()
        {
            shutdownBarrier.acquire();
            server.Shutdown();
        });
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
        auto configuration = NES::loadConfiguration<NES::SingleNodeWorkerConfiguration>(argc, argv);
        if (!configuration)
        {
            return 0;
        }
        {
            NES::GRPCServer workerService{NES::SingleNodeWorker(*configuration)};

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
