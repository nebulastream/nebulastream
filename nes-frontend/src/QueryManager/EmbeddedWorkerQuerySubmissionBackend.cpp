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
#include <memory>

#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryManager/QueryManager.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <folly/ScopeGuard.h>
#include <ErrorHandling.hpp>
#include <QueryStatus.hpp>
#include <QueryTerminationType.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <WorkerStatus.hpp>
#include <scope_guard.hpp>

namespace NES
{
auto updateWorkerNodeIdForScope(const Host& workerNodeId)
{
    const auto currentWorkerNodeId = std::exchange(Thread::WorkerNodeId, workerNodeId);
    return scope_guard::make_scope_exit([=]() { Thread::WorkerNodeId = currentWorkerNodeId; });
}

EmbeddedWorkerQuerySubmissionBackend::EmbeddedWorkerQuerySubmissionBackend(
    WorkerConfig config, SingleNodeWorkerConfiguration workerConfiguration)
    : worker{[&]()
             {
                 /// Start with the per-worker topology config, then overlay only
                 /// explicitly-set CLI values so that CLI args take highest priority
                 /// but topology values aren't clobbered by CLI defaults.
                 SingleNodeWorkerConfiguration mergedConfig = config.config;
                 mergedConfig.applyExplicitlySetFrom(workerConfiguration);

                 /// Set grpc/data from topology (these always come from cluster config)
                 mergedConfig.grpcAddressUri.setValue(config.host.getRawValue());
                 mergedConfig.dataAddress.setValue(config.dataAddress);

                 const LogContext logContext("create", config.host);
                 auto guard = updateWorkerNodeIdForScope(config.host);
                 return SingleNodeWorker(mergedConfig, config.host);
             }()}
    , config(std::move(config))
{
}

std::expected<QueryId, Exception> EmbeddedWorkerQuerySubmissionBackend::registerQuery(LogicalPlan plan)
{
    auto guard = updateWorkerNodeIdForScope(config.host);
    return worker.registerQuery(plan);
}

std::expected<void, Exception> EmbeddedWorkerQuerySubmissionBackend::start(QueryId queryId)
{
    auto guard = updateWorkerNodeIdForScope(config.host);
    return worker.startQuery(queryId);
}

std::expected<void, Exception> EmbeddedWorkerQuerySubmissionBackend::stop(QueryId queryId)
{
    auto guard = updateWorkerNodeIdForScope(config.host);
    return worker.stopQuery(queryId, QueryTerminationType::Graceful);
}

std::expected<LocalQueryStatusSnapshot, Exception> EmbeddedWorkerQuerySubmissionBackend::status(QueryId queryId) const
{
    auto guard = updateWorkerNodeIdForScope(config.host);
    return worker.getQueryStatus(queryId);
}

std::expected<WorkerStatus, Exception> EmbeddedWorkerQuerySubmissionBackend::workerStatus(std::chrono::system_clock::time_point after) const
{
    auto guard = updateWorkerNodeIdForScope(config.host);
    return worker.getWorkerStatus(after);
}

BackendProvider createEmbeddedBackend(const SingleNodeWorkerConfiguration& workerConfiguration)
{
    return [workerConfiguration](const WorkerConfig& config) /// NOLINT(bugprone-exception-escape)
    { return std::make_unique<EmbeddedWorkerQuerySubmissionBackend>(config, workerConfiguration); };
}

}
