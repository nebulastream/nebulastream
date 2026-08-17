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
#include <map>
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
#include <QueryManager/QueryManagementUtils.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Pointers.hpp>
#include <cpptrace/from_current_macros.hpp>
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

std::expected<folly::Synchronized<DistributedQuery>*, Exception> QueryManager::getQuery(DistributedQueryId query)
{
    auto it = state.queries.find(query);
    if (it == state.queries.end())
    {
        return std::unexpected(QueryNotFound("Query {} is not known to the QueryManager", query));
    }
    return &it->second;
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

namespace fs = std::filesystem;

QueryManager::QueryManager(SharedPtr<WorkerCatalog> workerCatalog, BackendProvider provider, QueryManagerState state)
    : state(std::move(state)), backends(std::move(workerCatalog), std::move(provider))
{
    // TODO tmp workaround, move somewhere else
    QueryOptimizerNetworkConfiguration networkConfig;
    auto base = networkConfig.backupBasePath.getValue();
    if (fs::exists(base) && fs::is_directory(base)) {
        for (const auto& entry : fs::directory_iterator(base)) {
            fs::remove_all(entry.path());
        }
    }
    fs::create_directories(base);
}

QueryManager::QueryManager(SharedPtr<WorkerCatalog> workerCatalog, BackendProvider provider)
    : backends(std::move(workerCatalog), std::move(provider))
{
    // TODO tmp workaround, move somewhere else
    QueryOptimizerNetworkConfiguration networkConfig;
    auto base = networkConfig.backupBasePath.getValue();
    if (fs::exists(base) && fs::is_directory(base)) {
        for (const auto& entry : fs::directory_iterator(base)) {
            fs::remove_all(entry.path());
        }
    }
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
    std::unordered_map<Host, std::vector<LocalQuery>> localQueries;

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
            try
            {
                /// Set the distributed query ID on the local plan before sending to worker
                localPlan.setQueryId(QueryId::createDistributed(id));
                const auto result = backends.at(host).start(localPlan);
                if (result)
                {
                    std::cout << "Sent query " << *result << " to host " << host.getRawValue() <<  std::endl;
                    NES_DEBUG("Starting query on node {} was successful.", host);
                    localQueries[host].emplace_back(host, *result, localPlan);
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
        /// Local plans that already started are not tracked in `state.queries` on this path, so nobody would ever stop them. A network
        /// sink whose downstream local plan never started finds no receiving end for its channel, so the receiver denies it. The
        /// sender treats a denied channel as a transient failure and retries forever, which blocks worker shutdown. Thus, we stop the
        /// partial deployment here.
        for (const auto& [host, startedQueries] : localQueries)
        {
            for (const auto& localQuery : startedQueries)
            {
                CPPTRACE_TRY
                {
                    if (const auto result = backends.at(host).stop(localQuery.getCurrentQueryId()); not result)
                    {
                        NES_WARNING(
                            "Could not stop local query {} on {} while unwinding a failed start: {}",
                            localQuery.getCurrentQueryId(),
                            host,
                            result.error());
                    }
                }
                CPPTRACE_CATCH(const std::exception& exception)
                {
                    NES_WARNING(
                        "Could not stop local query {} on {} while unwinding a failed start: {}",
                        localQuery.getCurrentQueryId(),
                        host,
                        exception.what());
                }
            }
        }
        return std::unexpected(exceptions);
    }

    this->state.queries.try_emplace(id, DistributedQuery(id, std::move(localQueries)));

    /// Poll until all local queries have advanced past Registered, so the caller can immediately
    /// observe a meaningful status after this function returns.
    auto& syncQuery = this->state.queries.at(id);
    auto query = syncQuery.wlock();

    auto waitForStatusChange = query->iterate()
        | std::views::transform([](const auto& pair) { return std::pair{std::get<0>(pair), &std::get<1>(pair)}; })
        | std::ranges::to<std::vector>();
    /// The query is expected to be moved into the started state pretty quickly after lowering, so we start with a rapid polling
    /// interval. If the system is overloaded the state change may take much longer, so we back off exponentially, but cap the
    /// interval at 500ms: unbounded backoff would leave long periods where no one observes the query status.
    constexpr auto initialStatusPollInterval = std::chrono::milliseconds(10);
    constexpr auto maxStatusPollInterval = std::chrono::milliseconds(500);
    constexpr auto statusPollTimeout = std::chrono::seconds(1000);
    const auto statusPollDeadline = std::chrono::steady_clock::now() + statusPollTimeout;
    for (auto pollInterval = initialStatusPollInterval;; pollInterval = std::min(pollInterval * 2, maxStatusPollInterval))
    {
        std::erase_if(
            waitForStatusChange,
            [&](const auto& pair)
            {
                auto [wId, localQuery] = pair;
                const auto result = backends.at(wId).status(localQuery->getCurrentQueryId());
                if (!result)
                {
                    exceptions.emplace_back(QueryStartFailed("Waiting for query state to change: {}", result.error()));
                    return true;
                }
                return result->state != QueryStatus::Registered;
            });

        if (waitForStatusChange.empty() or std::chrono::steady_clock::now() >= statusPollDeadline)
        {
            break;
        }
        std::this_thread::sleep_for(pollInterval);
    }

    if (!waitForStatusChange.empty())
    {
        exceptions.emplace_back(QueryStartFailed(
            "Query state did not change for local queries within {}: {}",
            statusPollTimeout,
            fmt::join(
                waitForStatusChange
                    | std::views::transform([](const auto& pair)
                                            { return fmt::format("{}@{}", std::get<1>(pair)->getCurrentQueryId(), std::get<0>(pair)); }),
                ", ")));
    }

    if (not exceptions.empty())
    {
        return std::unexpected{exceptions};
    }

    NES_DEBUG(
        "Query {} started successfully after {}.",
        id,
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - queryStartTimestamp));
    return id;
}

std::expected<DistributedQueryStatusSnapshot, std::vector<Exception>> QueryManager::status(const DistributedQueryId& queryId)
{
    auto queryResult = getQuery(queryId);
    if (!queryResult.has_value())
    {
        return std::unexpected(std::vector{queryResult.error()});
    }
    auto *syncQuery = queryResult.value();
    auto query = syncQuery->wlock();

    std::unordered_map<Host, std::unordered_map<QueryId, std::expected<LocalQueryStatusSnapshot, Exception>>> localStatusResults;

    for (auto [host, localQuery] : query->iterate())
    {
        QueryManagementUtils::checkLocalQueryStatus(localQuery, backends.at(localQuery.getHost()), false);
    }

    bool completed = query->checkQueryCompletion();

    for (auto [host, localQuery] : query->iterate())
    {
        auto localQueryId = localQuery.getCurrentQueryId();
        localStatusResults[host].emplace(localQueryId, localQuery.createProvisionalStatusSnapshot(completed));
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

std::map<Host, std::expected<VersionInfo, Exception>> QueryManager::workerVersions() const
{
    std::map<Host, std::expected<VersionInfo, Exception>> versions;
    for (const auto& [wId, backend] : backends)
    {
        versions.try_emplace(wId, backend->version());
    }
    return versions;
}

std::vector<DistributedQueryId> QueryManager::getRunningQueries()
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

std::expected<void, std::vector<Exception>> QueryManager::stop(DistributedQueryId queryId)
{
    auto queryResult = getQuery(std::move(queryId));
    if (!queryResult.has_value())
    {
        return std::unexpected(std::vector{queryResult.error()});
    }
    auto syncQuery = queryResult.value();
    auto query = syncQuery->wlock();

    std::vector<Exception> exceptions{};

    for (const auto& [host, localQuery] : query->iterate())
    {
        try
        {
            INVARIANT(backends.contains(host), "Local query references node ({}) that is not part of the cluster", host);
            auto result = backends.at(host).stop(localQuery.getCurrentQueryId());
            if (result)
            {
                NES_DEBUG("Stopping query {} on node {} was successful.", localQuery.getCurrentQueryId(), host);
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
    query->setCompleted();
    supervisors.erase(query->getDistributedQueryId());

    if (not exceptions.empty())
    {
        return std::unexpected{exceptions};
    }
    NES_DEBUG("Stopping query {} was successful.", queryId);
    state.queries.erase(queryId);
    return {};
}

std::expected<void, std::vector<Exception>> QueryManager::superviseNonBlocking(DistributedQueryId distributedQueryId)
{
    auto queryResult = getQuery(distributedQueryId);
    if (!queryResult.has_value())
    {
        return std::unexpected(std::vector{queryResult.error()});
    }
    auto [it, _] = supervisors.try_emplace(distributedQueryId, *this, *queryResult.value());
    it->second.begin();
    return {};
}


}
