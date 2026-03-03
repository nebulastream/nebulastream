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

#pragma once

#include <expected>
#include <memory>

#include <QueryManager/GRPCQuerySubmissionBackend.hpp>
#include <QueryManager/QueryManager.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

namespace NES
{

/// QueryManager that hosts the SingleNodeWorker in a forked child and talks to it over gRPC.
/// The child process can be force-killed to simulate a hard crash without tearing down the parent.
class ForkedWorkerQueryManager final : public QuerySubmissionBackend
{
public:
    explicit ForkedWorkerQueryManager(const SingleNodeWorkerConfiguration& configuration);
    ~ForkedWorkerQueryManager() override;

    ForkedWorkerQueryManager(const ForkedWorkerQueryManager&) = delete;
    ForkedWorkerQueryManager& operator=(const ForkedWorkerQueryManager&) = delete;
    ForkedWorkerQueryManager(ForkedWorkerQueryManager&&) = delete;
    ForkedWorkerQueryManager& operator=(ForkedWorkerQueryManager&&) = delete;

    [[nodiscard]] std::expected<QueryId, Exception> registerQuery(LogicalPlan plan) override;
    std::expected<void, Exception> start(QueryId queryId) override;
    std::expected<void, Exception> stop(QueryId queryId) override;
    std::expected<void, Exception> unregister(QueryId queryId) override;
    [[nodiscard]] std::expected<LocalQueryStatus, Exception> status(QueryId queryId) const override;
    [[nodiscard]] std::expected<WorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point after) const override;

    /// Immediately kill the child worker (SIGKILL). No cleanup is attempted.
    void crashWorker() noexcept;

    [[nodiscard]] bool isAlive() const noexcept { return childPid > 0; }

private:
    void spawnChild(const SingleNodeWorkerConfiguration& configuration);
    void shutdownChild(int signal) noexcept;
    void resetGrpcClient(const SingleNodeWorkerConfiguration& configuration, uint16_t port);

    pid_t childPid{-1};
    uint16_t listeningPort{0};
    std::unique_ptr<GRPCQuerySubmissionBackend> grpcManager;
};

}
