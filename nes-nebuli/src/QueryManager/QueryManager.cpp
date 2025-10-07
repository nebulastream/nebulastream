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

#include <QueryManager/QueryManager.hpp>

#include <utility>
#include <Util/Pointers.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
std::expected<DistributedQuery, Exception> QueryManager::getQuery(DistributedQueryId query) const
{
    const auto it = state.queries.find(query);
    if (it == state.queries.end())
    {
        return std::unexpected(QueryNotFound("Query {} is not known to the QueryManager", query));
    }
    return it->second;
}

QueryStatusNotifier::QueryStatusNotifier(QueryManager& owner) : owner(owner)
{
    thread = Thread(
        "QueryStatusNotifier",
        [this](std::stop_token token)
        {
            std::chrono::time_point<std::chrono::system_clock> nextUpdate{std::chrono::microseconds(0)};
            while (!token.stop_requested())
            {
                if (this->notifiers.rlock()->empty())
                {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    continue;
                }

                auto lastUpdate = std::exchange(nextUpdate, std::chrono::system_clock::now());
                auto result = this->owner.workerStatus(lastUpdate);
                auto state = this->notifiers.wlock();
                if (result)
                {
                    for (const auto& [grpc, workerStatusResult] : result->workerStatus)
                    {
                        if (workerStatusResult)
                        {
                            for (const auto& activeQuery : workerStatusResult->activeQueries)
                            {
                                auto it = state->find({grpc, activeQuery.queryId, QueryState::Running});
                                if (it != state->end())
                                {
                                    for (auto& notifier : it->second)
                                    {
                                        if (--notifier.waiting == 0)
                                        {
                                            notifier.promise.set_value();
                                        }
                                    }
                                    state->erase(it);
                                }
                            }
                            for (const auto& terminatedQuery : workerStatusResult->terminatedQueries)
                            {
                                auto it = state->find({grpc, terminatedQuery.queryId, QueryState::Stopped});
                                if (it != state->end())
                                {
                                    for (auto& notifier : it->second)
                                    {
                                        if (--notifier.waiting == 0)
                                        {
                                            notifier.promise.set_value();
                                        }
                                    }
                                    state->erase(it);
                                }
                            }
                        }
                    }
                }
            }
        });
}

std::unordered_map<GrpcAddr, UniquePtr<QuerySubmissionBackend>>
QueryManager::QueryManagerBackends::createBackends(const std::vector<WorkerConfig>& workers, BackendProvider& provider)
{
    std::unordered_map<GrpcAddr, UniquePtr<QuerySubmissionBackend>> backends;
    for (const auto& workerConfig : workers)
    {
        backends.emplace(workerConfig.grpc, provider(workerConfig));
    }
    return backends;
}

QueryManager::QueryManagerBackends::QueryManagerBackends(SharedPtr<WorkerCatalog> workerCatalog, BackendProvider provider)
    : workerCatalog(std::move(workerCatalog)), backendProvider(std::move(provider))
{
    rebuildBackendsIfNeeded();
}

QueryManager::QueryManager(SharedPtr<WorkerCatalog> workerCatalog, BackendProvider provider, QueryManagerState state)
    : state(std::move(state)), backends(std::move(workerCatalog), std::move(provider)), notifier(*this)
{
}

QueryManager::QueryManager(SharedPtr<WorkerCatalog> workerCatalog, BackendProvider provider)
    : backends(std::move(workerCatalog), std::move(provider)), notifier(*this)
{
}

void QueryManager::QueryManagerBackends::rebuildBackendsIfNeeded() const
{
    const auto currentVersion = workerCatalog->getVersion();
    if (currentVersion != cachedWorkerCatalogVersion)
    {
        NES_DEBUG("WorkerCatalog version changed from {} to {}, rebuilding backends", cachedWorkerCatalogVersion, currentVersion);
        backends = createBackends(workerCatalog->getAllWorkers(), backendProvider);
        cachedWorkerCatalogVersion = currentVersion;
    }
}

static DistributedQueryId uniqueDistributedQueryId(const QueryManagerState& state)
{
    auto uniqueId = getNextDistributedQueryId();
    size_t counter = 0;
    while (state.queries.contains(uniqueId))
    {
        uniqueId = DistributedQueryId(getNextDistributedQueryId().getRawValue() + std::to_string(counter++));
    }
    return uniqueId;
}

std::expected<DistributedQueryId, Exception> QueryManager::registerQuery(const PlanStage::DistributedLogicalPlan& plan)
{
    std::unordered_map<GrpcAddr, std::vector<LocalQueryId>> localQueries;

    auto id = plan.getQueryId();
    if (id == DistributedQueryId(DistributedQueryId::INVALID.value))
    {
        id = uniqueDistributedQueryId(state);
    }
    else if (this->state.queries.contains(plan.getQueryId()))
    {
        throw QueryAlreadyRegistered("{}", plan.getQueryId());
    }

    for (const auto& [grpcAddr, localPlans] : plan)
    {
        INVARIANT(backends.contains(grpcAddr), "Plan was assigned to a node ({}) that is not part of the cluster", grpcAddr);
        for (const auto& localPlan : localPlans)
        {
            try
            {
                const auto result = backends.at(grpcAddr).registerQuery(localPlan);
                if (result)
                {
                    NES_DEBUG("Registration to node {} was successful.", grpcAddr);
                    localQueries[grpcAddr].emplace_back(*result);
                    continue;
                }
                return std::unexpected{result.error()};
            }
            catch (const std::exception& e)
            {
                return std::unexpected{QueryRegistrationFailed("Message from external exception: {}", e.what())};
            }
        }
    }

    this->state.queries.emplace(id, std::move(localQueries));
    return id;
}

std::expected<void, std::vector<Exception>> QueryManager::start(DistributedQueryId queryId)
{
    auto queryResult = getQuery(queryId);
    if (!queryResult.has_value())
    {
        return std::unexpected(std::vector{queryResult.error()});
    }
    auto query = queryResult.value();
    std::vector<Exception> exceptions;

    for (const auto& [grpcAddr, localQueryId] : query.iterate())
    {
        try
        {
            INVARIANT(backends.contains(grpcAddr), "Local query references node ({}) that is not part of the cluster", grpcAddr);
            const auto result = backends.at(grpcAddr).start(localQueryId);
            if (result)
            {
                NES_DEBUG("Starting query {} on node {} was successful.", localQueryId, grpcAddr);
                continue;
            }

            exceptions.emplace_back(result.error());
        }
        catch (std::exception& e)
        {
            exceptions.emplace_back(QueryStartFailed("Message from external exception: {} ", e.what()));
        }
    }

    if (not exceptions.empty())
    {
        return std::unexpected{exceptions};
    }
    return {};
}

std::expected<DistributedQueryStatus, std::vector<Exception>> QueryManager::status(DistributedQueryId queryId) const
{
    auto queryResult = getQuery(queryId);
    if (!queryResult.has_value())
    {
        return std::unexpected(std::vector{queryResult.error()});
    }
    auto query = queryResult.value();

    std::unordered_map<GrpcAddr, std::unordered_map<LocalQueryId, std::expected<LocalQueryStatus, Exception>>> localStatusResults;

    for (const auto& [grpcAddr, localQueryId] : query.iterate())
    {
        try
        {
            INVARIANT(backends.contains(grpcAddr), "Local query references node ({}) that is not part of the cluster", grpcAddr);
            const auto result = backends.at(grpcAddr).status(localQueryId);
            localStatusResults[grpcAddr].emplace(localQueryId, result);
        }
        catch (std::exception& e)
        {
            localStatusResults[grpcAddr].emplace(
                localQueryId, std::unexpected(QueryStatusFailed("Message from external exception: {} ", e.what())));
        }
    }

    return DistributedQueryStatus{.localStatusSnapshots = localStatusResults, .queryId = queryId};
}

std::vector<DistributedQueryId> QueryManager::queries() const
{
    return state.queries | std::views::keys | std::ranges::to<std::vector>();
}

std::expected<DistributedWorkerStatus, Exception> QueryManager::workerStatus(std::chrono::system_clock::time_point after) const
{
    DistributedWorkerStatus distributedStatus;
    for (const auto& [grpcAddr, backend] : backends)
    {
        distributedStatus.workerStatus.try_emplace(grpcAddr, backend->workerStatus(after));
    }
    return distributedStatus;
}

std::vector<DistributedQueryId> QueryManager::getRunningQueries() const
{
    return state.queries | std::views::keys
        | std::views::transform(
               [this](const auto& id) -> std::optional<std::pair<DistributedQueryId, DistributedQueryStatus>>
               {
                   auto result = status(id);
                   if (result)
                   {
                       return std::optional<std::pair<DistributedQueryId, DistributedQueryStatus>>{{id, *result}};
                   }
                   return std::nullopt;
               })
        | std::views::filter([](auto idAndStatus) { return idAndStatus.has_value(); })
        | std::views::filter([](auto idAndStatus) { return idAndStatus->second.getGlobalQueryState() == DistributedQueryState::Running; })
        | std::views::transform([](auto idAndStatus) { return idAndStatus->first; }) | std::ranges::to<std::vector>();
}

std::expected<void, std::vector<Exception>> QueryManager::stop(DistributedQueryId queryId)
{
    auto queryResult = getQuery(queryId);
    if (!queryResult.has_value())
    {
        return std::unexpected(std::vector{queryResult.error()});
    }
    auto query = queryResult.value();

    std::vector<Exception> exceptions{};

    for (const auto& [grpcAddr, localQueryId] : query.iterate())
    {
        try
        {
            INVARIANT(backends.contains(grpcAddr), "Local query references node ({}) that is not part of the cluster", grpcAddr);
            auto result = backends.at(grpcAddr).stop(localQueryId);
            if (result)
            {
                NES_DEBUG("Stopping query {} on node {} was successful.", localQueryId, grpcAddr);
                continue;
            }
            exceptions.push_back(result.error());
        }
        catch (std::exception& e)
        {
            exceptions.push_back(QueryStopFailed("Message from external exception: {} ", e.what()));
        }
    }

    if (not exceptions.empty())
    {
        return std::unexpected{exceptions};
    }
    return {};
}

std::expected<void, std::vector<Exception>> QueryManager::unregister(DistributedQueryId queryId)
{
    auto queryResult = getQuery(queryId);
    if (!queryResult.has_value())
    {
        return std::unexpected(std::vector{queryResult.error()});
    }
    auto query = queryResult.value();
    std::vector<Exception> exceptions{};

    for (const auto& [grpcAddr, localQueryId] : query.iterate())
    {
        try
        {
            INVARIANT(backends.contains(grpcAddr), "Local query references node ({}) that is not part of the cluster", grpcAddr);
            auto result = backends.at(grpcAddr).unregister(localQueryId);
            if (result)
            {
                NES_DEBUG("Unregister of query {} on node {} was successful.", localQueryId, grpcAddr);
                continue;
            }
            exceptions.push_back(result.error());
        }
        catch (std::exception& e)
        {
            exceptions.push_back(QueryUnregistrationFailed("Message from external exception: {} ", e.what()));
        }
    }

    if (not exceptions.empty())
    {
        return std::unexpected{exceptions};
    }
    return {};
}

}
