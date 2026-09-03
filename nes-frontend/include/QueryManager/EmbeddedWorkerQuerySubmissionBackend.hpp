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
#include <memory>
#include <string>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <ErrorHandling.hpp>
#include <QueryStatus.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <Version.hpp>
#include <WorkerStatus.hpp>

namespace NES
{

namespace detail
{
/// Owns the worker thread and the request/reply channel that drives it.
/// Defined in the .cpp so the header doesn't pull in the variant alternatives,
/// folly queues, or the SingleNodeWorker.
class Channel;
}

class EmbeddedWorkerQuerySubmissionBackend final : public QuerySubmissionBackend
{
public:
    ~EmbeddedWorkerQuerySubmissionBackend() override;
    EmbeddedWorkerQuerySubmissionBackend(WorkerConfig config, SingleNodeWorkerConfiguration workerConfiguration);
    [[nodiscard]] std::expected<QueryId, Exception> start(LogicalPlan) override;
    std::expected<void, Exception> stop(QueryId) override;
    std::expected<void, Exception> terminate(QueryId) override;
    std::expected<void, Exception> resetWorker() override;
    std::expected<void, Exception> registerFailpoints(std::string& config) override;
    std::expected<std::vector<std::string>, Exception> checkFailpointsTriggered() override;
    [[nodiscard]] std::expected<LocalQueryStatusSnapshot, Exception> status(QueryId) const override;
    [[nodiscard]] std::expected<WorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point after) const override;
    [[nodiscard]] std::expected<VersionInfo, Exception> version() const override;

private:
    Host host;
    std::unique_ptr<detail::Channel> channel;
    void triggerRollback();
};

BackendProvider createEmbeddedBackend(const SingleNodeWorkerConfiguration& workerConfiguration);

}
