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

#include <QueryManager/EmbeddedWorkerQuerySubmissionBackend.hpp>

#include <chrono>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryManager/QueryManager.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <ErrorHandling.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <WorkerStatus.hpp>

namespace NES
{

EmbeddedWorkerQuerySubmissionBackend::EmbeddedWorkerQuerySubmissionBackend(
    WorkerConfig config, SingleNodeWorkerConfiguration workerConfiguration)
    : worker{[&]()
             {
                 workerConfiguration.grpcAddressUri = URI(config.grpc.getRawValue());
                 const LogContext logContext("create", config.grpc);
                 return SingleNodeWorker(workerConfiguration, WorkerId("embedded"));
             }()}
{
}

std::expected<LocalQueryId, Exception> EmbeddedWorkerQuerySubmissionBackend::registerQuery(LogicalPlan plan)
{
    return worker.registerQuery(plan);
}

std::expected<void, Exception> EmbeddedWorkerQuerySubmissionBackend::start(LocalQueryId queryId)
{
    return worker.startQuery(queryId);
}

std::expected<void, Exception> EmbeddedWorkerQuerySubmissionBackend::stop(LocalQueryId queryId)
{
    return worker.stopQuery(queryId, QueryTerminationType::Graceful);
}

std::expected<void, Exception> EmbeddedWorkerQuerySubmissionBackend::unregister(LocalQueryId queryId)
{
    return worker.unregisterQuery(queryId);
}

std::expected<LocalQueryStatus, Exception> EmbeddedWorkerQuerySubmissionBackend::status(LocalQueryId queryId) const
{
    return worker.getQueryStatus(queryId);
}

std::expected<WorkerStatus, Exception> EmbeddedWorkerQuerySubmissionBackend::workerStatus(std::chrono::system_clock::time_point after) const
{
    return worker.getWorkerStatus(after);
}

}
