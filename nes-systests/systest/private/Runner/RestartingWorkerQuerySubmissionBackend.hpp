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

#include <chrono>
#include <cstdint>
#include <expected>
#include <filesystem>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <Plans/LogicalPlan.hpp>
#include <QueryManager/GRPCQuerySubmissionBackend.hpp>
#include <QueryManager/QueryManager.hpp>
#include <sys/types.h>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>
#include <QueryStatus.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <Version.hpp>
#include <WorkerConfig.hpp>
#include <WorkerStatus.hpp>

namespace NES
{

/// Runs the worker as a child process whose file sources crash it after every n tuples they read. Whenever the worker crashed, the
/// backend restarts it with recovery from its checkpoints, so that the queries continue where their last checkpoint left off. Recovered
/// queries keep their ids, hence the backend assigns them before it starts a query.
class RestartingWorkerQuerySubmissionBackend final : public QuerySubmissionBackend
{
public:
    RestartingWorkerQuerySubmissionBackend(
        const WorkerConfig& config,
        const SingleNodeWorkerConfiguration& workerConfiguration,
        uint64_t crashEveryNTuples,
        std::filesystem::path workingDirectory);
    ~RestartingWorkerQuerySubmissionBackend() override;

    RestartingWorkerQuerySubmissionBackend(const RestartingWorkerQuerySubmissionBackend&) = delete;
    RestartingWorkerQuerySubmissionBackend& operator=(const RestartingWorkerQuerySubmissionBackend&) = delete;
    RestartingWorkerQuerySubmissionBackend(RestartingWorkerQuerySubmissionBackend&&) = delete;
    RestartingWorkerQuerySubmissionBackend& operator=(RestartingWorkerQuerySubmissionBackend&&) = delete;

    [[nodiscard]] std::expected<QueryId, Exception> start(LogicalPlan plan) override;
    std::expected<void, Exception> stop(QueryId queryId) override;
    [[nodiscard]] std::expected<LocalQueryStatusSnapshot, Exception> status(QueryId queryId) const override;
    [[nodiscard]] std::expected<WorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point after) const override;
    [[nodiscard]] std::expected<VersionInfo, Exception> version() const override;

private:
    /// Restarts the worker with recovery if it crashed.
    void ensureWorkerIsRunning() const;
    /// Returns whether the worker crashed. Any other termination of the worker fails the test.
    bool reapCrashedWorker() const;
    /// A failed request may be the first sign of a crash, which the process needs a moment to complete.
    bool waitForWorkerCrash() const;
    void startWorker(bool recoverFromCheckpoint) const;
    /// Returns false if the worker crashed before it accepted requests.
    [[nodiscard]] bool launchWorker(bool recoverFromCheckpoint) const;
    void stopWorker() const;
    void removeCheckpoint(const QueryId& queryId) const;

    SingleNodeWorkerConfiguration workerConfiguration;
    uint64_t crashEveryNTuples;
    std::filesystem::path workingDirectory;
    std::filesystem::path checkpointDirectory;

    mutable std::mutex mutex;
    mutable pid_t workerProcess = -1;
    mutable uint64_t numberOfRestarts = 0;
    mutable std::unique_ptr<GRPCQuerySubmissionBackend> worker;
    /// Once a query terminated, its checkpoint is gone, so a restarted worker would not know it anymore.
    mutable std::unordered_map<QueryId, LocalQueryStatusSnapshot> terminatedQueries;
    std::unordered_map<QueryId, uint64_t> restartsWhenStarted;
};

BackendProvider createRestartingWorkerBackend(
    const SingleNodeWorkerConfiguration& workerConfiguration, uint64_t crashEveryNTuples, const std::filesystem::path& workingDirectory);

}
