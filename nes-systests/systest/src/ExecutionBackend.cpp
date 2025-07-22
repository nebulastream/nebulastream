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

#include <ExecutionBackend.hpp>

#include <optional>
#include <ranges>
#include <string>
#include <utility>
#include <vector>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>

#include <Distributed/DistributedQueryId.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <YAML/YamlLoader.hpp>
#include <ErrorHandling.hpp>
#include <QueryPlanning.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <SystestState.hpp>

namespace NES::Systest
{

std::expected<Distributed::QueryId, std::vector<Exception>>
EmbeddedWorkerBackend::registerQuery(const QueryPlanner::FinalizedLogicalPlan& plan)
{
    /// TODO: Temporarily disabled serialization check due to config field order issue
    /// Make sure the queryplan is passed through serialization logic.
    const auto serialized = QueryPlanSerializationUtil::serializeQueryPlan(*plan);
    const auto deserialized = QueryPlanSerializationUtil::deserializeQueryPlan(serialized);
    
    return worker.registerQuery(deserialized)
        .transform(
            [](const QueryId id) -> Distributed::QueryId
            {
                return Distributed::QueryId{
                    .localQueries = {Distributed::LocalQueryId{.id = id.getRawValue(), .grpcAddr = std::string()}}};
            })
        .transform_error([](Exception&& exception) { return std::vector{std::move(exception)}; });
}

void EmbeddedWorkerBackend::startQuery(const Distributed::QueryId& query)
{
    worker.startQuery(QueryId{query.localQueries.front().id});
}

QueryStatus EmbeddedWorkerBackend::status(const Distributed::QueryId& query)
{
    const auto id = QueryId{query.localQueries.front().id};
    auto summ = worker.getQuerySummary(id);
    INVARIANT(summ.has_value(), "Query was not registered before requesting its status");
    return {.localStatusSnapshots = {std::move(*summ)}};
}

EmbeddedWorkerBackend::EmbeddedWorkerBackend(const SingleNodeWorkerConfiguration& configuration) : worker{configuration}
{
}

/// Returns unexpected if at least one grpc request returned an exception
std::expected<Distributed::QueryId, std::vector<Exception>> ClusterBackend::registerQuery(const QueryPlanner::FinalizedLogicalPlan& plan)
{
    std::vector<Distributed::LocalQueryId> localQueryIds;
    localQueryIds.reserve(plan.size());
    std::vector<Exception> exceptions;

    for (const auto& [grpcAddr, localPlans] : plan)
    {
        try
        {
            for (const auto& localPlan : localPlans)
            {
                const auto queryId = cluster.at(grpcAddr).client.registerQuery(localPlan);
                localQueryIds.emplace_back(queryId.getRawValue(), grpcAddr);
            }
        }
        catch (const Exception& exception)
        {
            exceptions.emplace_back(exception);
        }
    }
    if (not exceptions.empty())
    {
        return std::unexpected(std::move(exceptions));
    }
    return Distributed::QueryId{.localQueries = localQueryIds};
}

void ClusterBackend::startQuery(const Distributed::QueryId& query)
{
    for (const auto& [localId, grpcAddr] : query)
    {
        INVARIANT(cluster.contains(grpcAddr), "Cluster node with grpc endpoint {} has not been registered", grpcAddr);
        cluster.at(grpcAddr).client.start(QueryId{localId});
    }
}

QueryStatus ClusterBackend::status(const Distributed::QueryId& query)
{
    std::vector<LocalQueryStatus> localQuerySummaries;
    localQuerySummaries.reserve(query.localQueries.size());

    for (const auto& [localId, grpcAddr] : query)
    {
        INVARIANT(cluster.contains(grpcAddr), "Cluster node with grpc endpoint {} has not been registered", grpcAddr);
        auto localQuerySummary = cluster.at(grpcAddr).client.status(QueryId{localId});
        localQuerySummaries.push_back(std::move(localQuerySummary));
    }
    return {localQuerySummaries};
}

ClusterBackend::ClusterBackend(const std::string& clusterConfigFile)
    : cluster{
          CLI::YamlLoader<CLI::ClusterConfig>::load(clusterConfigFile).nodes
          | std::views::transform(
              [](const auto& node)
              {
                  return std::make_pair(
                      node.grpc,
                      ClusterNode{
                          .client = GRPCClient{grpc::CreateChannel(node.grpc, grpc::InsecureChannelCredentials())}, .workerConfig = node});
              })
          | std::ranges::to<std::unordered_map>()}
{
}

}
