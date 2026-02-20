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

#include <QueryManager/ForkedWorkerQueryManager.hpp>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstring>
#include <string>
#include <thread>
#include <fcntl.h>
#include <unistd.h>
#include <sys/wait.h>

#include <fmt/format.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/server_builder.h>

#include <Util/Logger/Logger.hpp>
#include <Util/URI.hpp>
#include <ErrorHandling.hpp>
#include <GrpcService.hpp>

namespace
{
/// child process: run gRPC worker server and report the selected port to parent via pipeFd
[[noreturn]] void runWorkerChild(NES::SingleNodeWorkerConfiguration configuration, int pipeFd)
{
    /// Bind to any available port unless caller configured something else.
    configuration.grpcAddressUri.setValue(NES::URI("localhost:0"));

    NES::GRPCServer workerService{NES::SingleNodeWorker(configuration)};

    grpc::ServerBuilder builder;
    builder.SetMaxMessageSize(-1);
    int selectedPort = 0;
    builder.AddListeningPort(configuration.grpcAddressUri.getValue().toString(), grpc::InsecureServerCredentials(), &selectedPort);
    builder.RegisterService(&workerService);
    grpc::EnableDefaultHealthCheckService(true);

    auto server = builder.BuildAndStart();
    if (!server || selectedPort <= 0)
    {
        selectedPort = -1;
        (void)::write(pipeFd, &selectedPort, sizeof(selectedPort));
        ::close(pipeFd);
        _exit(1);
    }

    (void)::write(pipeFd, &selectedPort, sizeof(selectedPort));
    ::close(pipeFd);
    server->Wait();
    _exit(0);
}

int readPortFromChild(int fd)
{
    /// Non-blocking read with small timeout to avoid hanging if the child dies.
    const int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    int port = -1;
    while (std::chrono::steady_clock::now() < deadline)
    {
        const auto bytes = ::read(fd, &port, sizeof(port));
        if (bytes == static_cast<ssize_t>(sizeof(port)))
        {
            break;
        }
        if (bytes == -1 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            continue;
        }
        break;
    }
    return port;
}
}

namespace NES
{

ForkedWorkerQueryManager::ForkedWorkerQueryManager(const SingleNodeWorkerConfiguration& configuration)
{
    spawnChild(configuration);
}

ForkedWorkerQueryManager::~ForkedWorkerQueryManager()
{
    shutdownChild(SIGTERM);
}

void ForkedWorkerQueryManager::spawnChild(const SingleNodeWorkerConfiguration& configuration)
{
    int pipeFds[2]{-1, -1};
    if (::pipe(pipeFds) != 0)
    {
        throw CannotStartQueryEngine("Failed to create pipe for forked worker: {}", strerror(errno));
    }

    const pid_t pid = ::fork();
    if (pid == -1)
    {
        ::close(pipeFds[0]);
        ::close(pipeFds[1]);
        throw CannotStartQueryEngine("Failed to fork worker process: {}", strerror(errno));
    }

    if (pid == 0)
    {
        ::close(pipeFds[0]);
        runWorkerChild(configuration, pipeFds[1]);
    }

    childPid = pid;
    ::close(pipeFds[1]);
    const int port = readPortFromChild(pipeFds[0]);
    ::close(pipeFds[0]);

    if (port <= 0)
    {
        shutdownChild(SIGKILL);
        throw CannotStartQueryEngine("Forked worker failed to start gRPC server");
    }

    listeningPort = static_cast<uint16_t>(port);
    resetGrpcClient(listeningPort);
}

void ForkedWorkerQueryManager::resetGrpcClient(const uint16_t port)
{
    const auto address = fmt::format("localhost:{}", port);
    grpcManager
        = std::make_unique<GRPCQueryManager>(grpc::CreateChannel(address, grpc::InsecureChannelCredentials()), std::chrono::seconds{5});
}

void ForkedWorkerQueryManager::shutdownChild(const int signal) noexcept
{
    if (childPid > 0)
    {
        ::kill(childPid, signal);
        int status = 0;
        (void)::waitpid(childPid, &status, 0);
        childPid = -1;
    }
    grpcManager.reset();
}

std::expected<QueryId, Exception> ForkedWorkerQueryManager::registerQuery(const LogicalPlan& plan)
{
    return grpcManager->registerQuery(plan);
}

std::expected<void, Exception> ForkedWorkerQueryManager::start(const QueryId queryId) noexcept
{
    return grpcManager->start(queryId);
}

std::expected<void, Exception> ForkedWorkerQueryManager::stop(const QueryId queryId) noexcept
{
    return grpcManager->stop(queryId);
}

std::expected<void, Exception> ForkedWorkerQueryManager::unregister(const QueryId queryId) noexcept
{
    return grpcManager->unregister(queryId);
}

std::expected<LocalQueryStatus, Exception> ForkedWorkerQueryManager::status(const QueryId queryId) const noexcept
{
    return grpcManager->status(queryId);
}

void ForkedWorkerQueryManager::crashWorker() noexcept
{
    if (childPid > 0)
    {
        ::kill(childPid, SIGKILL);
        int status = 0;
        (void)::waitpid(childPid, &status, 0);
        childPid = -1;
    }
}

}
