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

#include <QueryManager/QueryManager.hpp>

#include <chrono>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <ErrorHandling.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <WorkerStatus.hpp>

namespace NES
{
class EmbeddedWorkerQuerySubmissionBackend final : public QuerySubmissionBackend
{
public:
    EmbeddedWorkerQuerySubmissionBackend(WorkerConfig config, SingleNodeWorkerConfiguration workerConfiguration);
    [[nodiscard]] std::expected<LocalQueryId, Exception> registerQuery(LogicalPlan) override;
    std::expected<void, Exception> start(LocalQueryId) override;
    std::expected<void, Exception> stop(LocalQueryId) override;
    std::expected<void, Exception> unregister(LocalQueryId) override;
    [[nodiscard]] std::expected<LocalQueryStatus, Exception> status(LocalQueryId) const override;
    [[nodiscard]] std::expected<WorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point after) const override;

    ~EmbeddedWorkerQuerySubmissionBackend() override;
    EmbeddedWorkerQuerySubmissionBackend(const EmbeddedWorkerQuerySubmissionBackend& other) = delete;
    EmbeddedWorkerQuerySubmissionBackend(EmbeddedWorkerQuerySubmissionBackend&& other) noexcept = delete;
    EmbeddedWorkerQuerySubmissionBackend& operator=(const EmbeddedWorkerQuerySubmissionBackend& other) = delete;
    EmbeddedWorkerQuerySubmissionBackend& operator=(EmbeddedWorkerQuerySubmissionBackend&& other) noexcept = delete;

private:
    WorkerId id;
    std::optional<SingleNodeWorker> worker;
};

BackendProvider createEmbeddedBackend(const SingleNodeWorkerConfiguration& workerConfiguration);

}
