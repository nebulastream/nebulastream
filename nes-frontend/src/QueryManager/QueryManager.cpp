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

#include <chrono>
#include <cmath>
#include <cstddef>
#include <exception>
#include <optional>
#include <ranges>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Pointers.hpp>
#include <fmt/ranges.h>
#include <DistributedLogicalPlan.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>
#include <QueryId.hpp>
#include <QueryStatus.hpp>
#include <WorkerCatalog.hpp>
#include <WorkerConfig.hpp>

namespace NES
{
namespace
{
DistributedQueryId uniqueDistributedQueryId(const QueryManagerState& state)
{
    auto uniqueId = getNextDistributedQueryId();
    size_t counter = 0;
    while (state.queries.contains(uniqueId))
    {
        uniqueId = DistributedQueryId(getNextDistributedQueryId().getRawValue() + std::to_string(counter++));
    }
    return uniqueId;
}
}

std::expected<DistributedQuery, Exception> QueryManager::getQuery(DistributedQueryId query) const
{
    const auto it = state.queries.find(query);
    if (it == state.queries.end())
    {
        return std::unexpected(QueryNotFound("Query {} is not known to the QueryManager", query));
    }
    return it->second;
}

std::unordered_map<Host, UniquePtr<QuerySubmissionBackend>>
QueryManager::QueryManagerBackends::createBackends(const std::vector<WorkerConfig>& workers, BackendProvider& provider)
{
    std::unordered_map<Host, UniquePtr<QuerySubmissionBackend>> backends;
    for (const auto& workerConfig : workers)
    {
        backends.emplace(workerConfig.host, provider(workerConfig));
    }
    return backends;
}

QueryManager::QueryManagerBackends::QueryManagerBackends(SharedPtr<WorkerCatalog> workerCatalog, BackendProvider provider)
    : workerCatalog(std::move(workerCatalog)), backendProvider(std::move(provider))
{
    rebuildBackendsIfNeeded();
}

QueryManager::QueryManager(SharedPtr<WorkerCatalog> workerCatalog, BackendProvider provider, QueryManagerState state)
    : state(std::move(state)), backends(std::move(workerCatalog), std::move(provider))
{
}

QueryManager::QueryManager(SharedPtr<WorkerCatalog> workerCatalog, BackendProvider provider)
    : backends(std::move(workerCatalog), std::move(provider))
{
}

void QueryManager::QueryManagerBackends::rebuildBackendsIfNeeded() const
{
    /// Lazily creates/rebuilds query submission backends (embedded or gRPC).
    /// Backends are re-created when the worker catalog version changes
    /// (workers added/removed) to pick up topology updates.
    const auto currentVersion = workerCatalog->getVersion();
    if (currentVersion != cachedWorkerCatalogVersion)
    {
        NES_DEBUG("WorkerCatalog version changed from {} to {}, rebuilding backends", cachedWorkerCatalogVersion, currentVersion);
        backends = createBackends(workerCatalog->getAllWorkers(), backendProvider);
        cachedWorkerCatalogVersion = currentVersion;
    }
}

[[nodiscard]] std::expected<DistributedQueryId, std::vector<Exception>> QueryManager::start(const DistributedLogicalPlan& plan)
{
    return start(plan, std::chrono::steady_clock::time_point::max(), {});
}

[[nodiscard]] std::expected<DistributedQueryId, std::vector<Exception>> QueryManager::start(
    const DistributedLogicalPlan& plan, const std::chrono::steady_clock::time_point deadline, const std::stop_token stopToken)
{
    if (stopToken.stop_requested() || std::chrono::steady_clock::now() >= deadline)
    {
        return std::unexpected(std::vector{QueryStartFailed("Query start was cancelled or reached its deadline")});
    }
    for (const auto& [host, localPlans] : plan)
    {
        static_cast<void>(localPlans);
        if (!backends.contains(host))
        {
            return std::unexpected(std::vector{QueryStartFailed("Plan was assigned to node {} which is not part of the cluster", host)});
        }
    }

    std::unordered_map<Host, std::vector<QueryId>> localQueries;

    auto id = plan.getQueryId();
    if (id == DistributedQueryId(DistributedQueryId::INVALID))
    {
        id = uniqueDistributedQueryId(state);
    }
    else if (this->state.queries.contains(plan.getQueryId()))
    {
        throw QueryAlreadyRegistered("{}", plan.getQueryId());
    }

    const std::chrono::steady_clock::time_point queryStartTimestamp = std::chrono::steady_clock::now();
    std::vector<Exception> exceptions;

    for (const auto& [host, localPlans] : plan)
    {
        INVARIANT(backends.contains(host), "Plan was assigned to a node ({}) that is not part of the cluster", host);
        for (auto localPlan : localPlans)
        {
            if (stopToken.stop_requested() || std::chrono::steady_clock::now() >= deadline)
            {
                exceptions.emplace_back(QueryStartFailed("Query start was cancelled or reached its deadline"));
                break;
            }
            try
            {
                const auto localQueryId = QueryId::create(LocalQueryId(generateUUID()), id);
                localPlan.setQueryId(localQueryId);
                localQueries[host].emplace_back(localQueryId);
                const auto result = backends.at(host).start(std::move(localPlan), deadline, stopToken);
                if (result)
                {
                    if (*result != localQueryId)
                    {
                        localQueries[host].back() = *result;
                    }
                    NES_DEBUG("Starting query on node {} was successful.", host);
                    continue;
                }
                exceptions.emplace_back(result.error());
            }
            catch (const std::exception& e)
            {
                exceptions.emplace_back(QueryStartFailed("Message from external exception: {}", e.what()));
            }
        }
    }

    if (!exceptions.empty())
    {
        if (!localQueries.empty())
        {
            state.queries.emplace(id, std::move(localQueries));
            pendingCleanupQueries.insert(id);
        }
        return std::unexpected(exceptions);
    }

    this->state.queries.emplace(id, std::move(localQueries));

    /// Poll until all local queries have advanced past Registered, so the caller can immediately
    /// observe a meaningful status after this function returns.
    auto query = this->state.queries.at(id);
    auto waitForStatusChange = query.iterate()
        | std::views::transform([](const auto& pair) { return std::pair{std::get<0>(pair), std::get<1>(pair)}; })
        | std::ranges::to<std::vector>();
    /// The query is expected to be moved into the started state pretty quickly after lowering, so we start with a rapid polling
    /// interval. If the system is overloaded the state change may take much longer, so we back off exponentially, but cap the
    /// interval at 500ms: unbounded backoff would leave long periods where no one observes the query status.
    constexpr auto initialStatusPollInterval = std::chrono::milliseconds(10);
    constexpr auto maxStatusPollInterval = std::chrono::milliseconds(500);
    constexpr auto statusPollTimeout = std::chrono::seconds(1000);
    const auto statusPollDeadline = std::min(deadline, std::chrono::steady_clock::now() + statusPollTimeout);
    for (auto pollInterval = initialStatusPollInterval;; pollInterval = std::min(pollInterval * 2, maxStatusPollInterval))
    {
        std::erase_if(
            waitForStatusChange,
            [&](const auto& pair)
            {
                auto [wId, localQueryId] = pair;
                const auto result = backends.at(wId).status(localQueryId, statusPollDeadline, stopToken);
                if (!result)
                {
                    exceptions.emplace_back(QueryStartFailed("Waiting for query state to change: {}", result.error()));
                    return true;
                }
                return result->state != QueryStatus::Registered;
            });

        if (waitForStatusChange.empty() || stopToken.stop_requested() || std::chrono::steady_clock::now() >= statusPollDeadline)
        {
            break;
        }
        std::this_thread::sleep_for(std::min(
            pollInterval, std::chrono::duration_cast<std::chrono::milliseconds>(statusPollDeadline - std::chrono::steady_clock::now())));
    }

    if (!waitForStatusChange.empty())
    {
        exceptions.emplace_back(QueryStartFailed(
            "Query state did not change for local queries within {}: {}",
            statusPollTimeout,
            fmt::join(
                waitForStatusChange
                    | std::views::transform([](const auto& pair) { return fmt::format("{}@{}", std::get<1>(pair), std::get<0>(pair)); }),
                ", ")));
    }

    if (not exceptions.empty())
    {
        pendingCleanupQueries.insert(id);
        return std::unexpected{exceptions};
    }

    NES_DEBUG(
        "Query {} started successfully after {}.",
        id,
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - queryStartTimestamp));
    return id;
}

std::expected<DistributedQueryStatusSnapshot, std::vector<Exception>> QueryManager::status(const DistributedQueryId& queryId) const
{
    return status(queryId, std::chrono::steady_clock::time_point::max(), {});
}

std::expected<DistributedQueryStatusSnapshot, std::vector<Exception>> QueryManager::status(
    const DistributedQueryId& queryId, const std::chrono::steady_clock::time_point deadline, const std::stop_token stopToken) const
{
    if (stopToken.stop_requested() || std::chrono::steady_clock::now() >= deadline)
    {
        return std::unexpected(std::vector{QueryStatusFailed("Query status request was cancelled or reached its deadline")});
    }
    auto queryResult = getQuery(queryId);
    if (!queryResult.has_value())
    {
        return std::unexpected(std::vector{queryResult.error()});
    }
    auto query = queryResult.value();

    std::unordered_map<Host, std::unordered_map<QueryId, std::expected<LocalQueryStatusSnapshot, Exception>>> localStatusResults;

    for (const auto& [host, localQueryId] : query.iterate())
    {
        try
        {
            INVARIANT(backends.contains(host), "Local query references node ({}) that is not part of the cluster", host);
            const auto result = backends.at(host).status(localQueryId, deadline, stopToken);
            localStatusResults[host].emplace(localQueryId, result);
        }
        /// Worker backends return std::expected for normal errors; this catch is defensive
        /// against unexpected exceptions from the underlying network layer (e.g., gRPC stubs).
        catch (const std::exception& e)
        {
            localStatusResults[host].emplace(
                localQueryId, std::unexpected(QueryStatusFailed("Message from external exception: {} ", e.what())));
        }
    }

    return DistributedQueryStatusSnapshot{.localStatusSnapshots = localStatusResults, .queryId = queryId};
}

std::vector<DistributedQueryId> QueryManager::queries() const
{
    return state.queries | std::views::keys | std::ranges::to<std::vector>();
}

std::expected<DistributedWorkerStatus, Exception> QueryManager::workerStatus(std::chrono::system_clock::time_point after) const
{
    DistributedWorkerStatus distributedStatus;
    for (const auto& [wId, backend] : backends)
    {
        distributedStatus.workerStatus.try_emplace(wId, backend->workerStatus(after));
    }
    return distributedStatus;
}

std::vector<DistributedQueryId> QueryManager::getRunningQueries() const
{
    return state.queries | std::views::keys
        | std::views::transform(
               [this](const auto& id) -> std::optional<std::pair<DistributedQueryId, DistributedQueryStatusSnapshot>>
               {
                   auto result = status(id);
                   if (result)
                   {
                       return std::optional<std::pair<DistributedQueryId, DistributedQueryStatusSnapshot>>{{id, *result}};
                   }
                   return std::nullopt;
               })
        | std::views::filter([](const auto& idAndStatus) { return idAndStatus.has_value(); })
        | std::views::filter([](auto idAndStatus) { return idAndStatus->second.getGlobalQueryStatus() == DistributedQueryStatus::Running; })
        | std::views::transform([](auto idAndStatus) { return idAndStatus->first; }) | std::ranges::to<std::vector>();
}

std::expected<void, std::vector<Exception>> QueryManager::cleanup(DistributedQueryId queryId)
{
    return cleanup(std::move(queryId), std::chrono::steady_clock::time_point::max(), {});
}

std::expected<void, std::vector<Exception>>
QueryManager::cleanup(DistributedQueryId queryId, const std::chrono::steady_clock::time_point deadline, const std::stop_token stopToken)
{
    auto queryResult = getQuery(queryId);
    if (!queryResult)
    {
        return std::unexpected(std::vector{queryResult.error()});
    }

    std::vector<Exception> exceptions;
    for (const auto& [host, localQueryId] : queryResult->iterate())
    {
        try
        {
            INVARIANT(backends.contains(host), "Local query references node ({}) that is not part of the cluster", host);
            const auto statusResult = backends.at(host).status(localQueryId, deadline, stopToken);
            if ((statusResult && (statusResult->state == QueryStatus::Stopped || statusResult->state == QueryStatus::Failed))
                || (!statusResult && statusResult.error().code() == ErrorCode::QueryNotFound))
            {
                continue;
            }
            if (auto stopped = backends.at(host).stop(localQueryId, deadline, stopToken);
                !stopped && stopped.error().code() != ErrorCode::QueryNotFound)
            {
                exceptions.push_back(stopped.error());
            }
        }
        catch (const std::exception& e)
        {
            exceptions.push_back(QueryStopFailed("Message from external exception: {} ", e.what()));
        }
    }

    if (!exceptions.empty())
    {
        return std::unexpected(std::move(exceptions));
    }
    pendingCleanupQueries.erase(queryId);
    state.queries.erase(queryId);
    return {};
}

std::expected<void, std::vector<Exception>> QueryManager::cleanupPending(const std::chrono::steady_clock::time_point deadline)
{
    std::vector<Exception> exceptions;
    const auto queriesToClean = pendingCleanupQueries | std::ranges::to<std::vector>();
    for (const auto& queryId : queriesToClean)
    {
        if (auto cleaned = cleanup(queryId, deadline, {}); !cleaned)
        {
            exceptions.insert(exceptions.end(), cleaned.error().begin(), cleaned.error().end());
        }
    }
    if (!exceptions.empty())
    {
        return std::unexpected(std::move(exceptions));
    }
    return {};
}

std::expected<void, std::vector<Exception>> QueryManager::stop(DistributedQueryId queryId)
{
    return stop(std::move(queryId), std::chrono::steady_clock::time_point::max(), {});
}

std::expected<void, std::vector<Exception>>
QueryManager::stop(DistributedQueryId queryId, const std::chrono::steady_clock::time_point deadline, const std::stop_token stopToken)
{
    auto queryResult = getQuery(queryId);
    if (!queryResult.has_value())
    {
        return std::unexpected(std::vector{queryResult.error()});
    }
    auto query = queryResult.value();

    std::vector<Exception> exceptions{};

    for (const auto& [host, localQueryId] : query.iterate())
    {
        try
        {
            INVARIANT(backends.contains(host), "Local query references node ({}) that is not part of the cluster", host);
            auto result = backends.at(host).stop(localQueryId, deadline, stopToken);
            if (result)
            {
                NES_DEBUG("Stopping query {} on node {} was successful.", localQueryId, host);
                continue;
            }
            exceptions.push_back(result.error());
        }
        /// Worker backends return std::expected for normal errors; this catch is defensive
        /// against unexpected exceptions from the underlying network layer (e.g., gRPC stubs).
        catch (const std::exception& e)
        {
            exceptions.push_back(QueryStopFailed("Message from external exception: {} ", e.what()));
        }
    }

    if (not exceptions.empty())
    {
        return std::unexpected{exceptions};
    }
    NES_DEBUG("Stopping query {} was successful.", queryId);
    pendingCleanupQueries.erase(queryId);
    state.queries.erase(queryId);
    return {};
}

void QueryManager::release(const DistributedQueryId queryId)
{
    pendingCleanupQueries.erase(queryId);
    state.queries.erase(queryId);
}

void QueryManager::shutdown(const std::chrono::steady_clock::time_point deadline)
{
    for (auto& [host, backend] : backends)
    {
        static_cast<void>(host);
        backend->shutdown(deadline);
    }
}

}
