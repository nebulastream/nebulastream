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

#include <Listeners/QueryLog.hpp>
#include <ErrorHandling.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

namespace NES
{
class EmbeddedWorkerQuerySubmissionBackend final : public QuerySubmissionBackend
{
public:
    EmbeddedWorkerQuerySubmissionBackend(WorkerConfig config, SingleNodeWorkerConfiguration workerConfiguration);
    [[nodiscard]] std::expected<QueryId, Exception> registerQuery(LogicalPlan) override;
    std::expected<void, Exception> start(QueryId) override;
    std::expected<void, Exception> stop(QueryId) override;
    std::expected<void, Exception> unregister(QueryId) override;
    [[nodiscard]] std::expected<LocalQueryStatus, Exception> status(QueryId) const override;
    [[nodiscard]] std::expected<WorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point after) const override;

private:
    SingleNodeWorker worker;
};

}
