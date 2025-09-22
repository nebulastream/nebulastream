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

#include <vector>

#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <DistributedQueryId.hpp>
#include <ErrorHandling.hpp>
#include <QueryPlanning.hpp>
#include <SingleNodeWorkerConfiguration.hpp>

namespace NES
{

EmbeddedWorkerQuerySubmissionBackend::EmbeddedWorkerQuerySubmissionBackend(
    std::vector<std::pair<WorkerConfig, SingleNodeWorkerConfiguration>> workers)
    : cluster{
          std::views::transform(
              workers,
              [&](const auto& configs)
              {
                  auto [networkConfig, localConfiguration] = configs;
                  localConfiguration.connection = networkConfig.host;
                  localConfiguration.grpcAddressUri = networkConfig.grpc;
                  LogContext logContext("create", networkConfig.grpc);
                  return std::make_pair(networkConfig.grpc, SingleNodeWorker(localConfiguration, WorkerId(networkConfig.host)));
              })
          | std::ranges::to<std::unordered_map>()}
{
}

EmbeddedWorkerQuerySubmissionBackend::EmbeddedWorkerQuerySubmissionBackend(
    std::vector<WorkerConfig> configs, const SingleNodeWorkerConfiguration& globalConfiguration)
    : EmbeddedWorkerQuerySubmissionBackend(
          std::views::transform(
              configs, [&](auto config) -> std::pair<WorkerConfig, SingleNodeWorkerConfiguration> { return {config, globalConfiguration}; })
          | std::ranges::to<std::vector>())

{
}

std::expected<LocalQueryId, Exception> EmbeddedWorkerQuerySubmissionBackend::registerQuery(const GrpcAddr& grpc, LogicalPlan plan)
{
    LogContext logContext("register", grpc);
    return getWorker(grpc).registerQuery(plan);
}

std::expected<void, Exception> EmbeddedWorkerQuerySubmissionBackend::start(const LocalQuery& query)
{
    LogContext logContext("start", query.grpcAddr);
    return getWorker(query.grpcAddr).startQuery(query.id);
}

std::expected<void, Exception> EmbeddedWorkerQuerySubmissionBackend::stop(const LocalQuery& query)
{
    LogContext logContext("stop", query.grpcAddr);
    return getWorker(query.grpcAddr).stopQuery(query.id, QueryTerminationType::Graceful);
}

std::expected<void, Exception> EmbeddedWorkerQuerySubmissionBackend::unregister(const LocalQuery& query)
{
    LogContext logContext("unregister", query.grpcAddr);
    return getWorker(query.grpcAddr).unregisterQuery(query.id);
}

std::expected<LocalQueryStatus, Exception> EmbeddedWorkerQuerySubmissionBackend::status(const LocalQuery& query) const
{
    LogContext logContext("status", query.grpcAddr);
    return getWorker(query.grpcAddr).getQueryStatus(query.id);
}

const SingleNodeWorker& EmbeddedWorkerQuerySubmissionBackend::getWorker(const GrpcAddr& grpc) const
{
    const auto it = cluster.find(grpc);
    if (it == cluster.end())
    {
        throw TestException("No worker found grpc address: {}", grpc);
    }
    return it->second;
}

SingleNodeWorker& EmbeddedWorkerQuerySubmissionBackend::getWorker(const GrpcAddr& grpc)
{
    return const_cast<SingleNodeWorker&>(const_cast<const EmbeddedWorkerQuerySubmissionBackend&>(*this).getWorker(grpc));
}

}
