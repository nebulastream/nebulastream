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
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <ErrorHandling.hpp>
#include <QueryStatus.hpp>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <WorkerStatus.hpp>

namespace NES
{
class AbstractStatisticStore;

class EmbeddedWorkerQuerySubmissionBackend final : public QuerySubmissionBackend
{
public:
    /// @param statisticStore optional statistic store injected into the worker so statistic build/probe queries can
    /// read/write it. nullptr (the default) means the worker cannot serve statistic queries.
    EmbeddedWorkerQuerySubmissionBackend(
        WorkerConfig config,
        SingleNodeWorkerConfiguration workerConfiguration,
        std::shared_ptr<AbstractStatisticStore> statisticStore = nullptr);
    [[nodiscard]] std::expected<QueryId, Exception> registerQuery(LogicalPlan) override;
    std::expected<void, Exception> start(QueryId) override;
    std::expected<void, Exception> stop(QueryId) override;
    [[nodiscard]] std::expected<LocalQueryStatusSnapshot, Exception> status(QueryId) const override;
    [[nodiscard]] std::expected<WorkerStatus, Exception> workerStatus(std::chrono::system_clock::time_point after) const override;

private:
    SingleNodeWorker worker;
};

/// @param statisticStore optional statistic store injected into the embedded worker (see the backend ctor).
BackendProvider
createEmbeddedBackend(const SingleNodeWorkerConfiguration& workerConfiguration, std::shared_ptr<AbstractStatisticStore> statisticStore = nullptr);

}
