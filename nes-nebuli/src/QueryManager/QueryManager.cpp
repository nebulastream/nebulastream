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

namespace NES
{
std::expected<Query, Exception> QueryManager::getQuery(DistributedQueryId query) const
{
    const auto it = state.queries.find(query);
    if (it == state.queries.end())
    {
        return std::unexpected(QueryNotFound("Query {} is not known to the QueryManager", query));
    }
    return it->second;
}

QueryManager::QueryManager(UniquePtr<QuerySubmissionBackend> backend, QueryManagerState state)
    : backend(std::move(backend)), state(std::move(state))
{
}

QueryManager::QueryManager(UniquePtr<QuerySubmissionBackend> backend) : backend(std::move(backend))
{
}

std::expected<DistributedQueryId, Exception> QueryManager::registerQuery(const PlanStage::DistributedLogicalPlan& plan)
{
    std::vector<LocalQuery> localQueries;
    localQueries.reserve(plan.size());

    for (const auto& [grpcAddr, localPlans] : plan)
    {
        for (const auto& localPlan : localPlans)
        {
            try
            {
                const auto result = backend->registerQuery(grpcAddr, localPlan);
                if (result)
                {
                    NES_DEBUG("Registration of local query {} to node {} was successful.", localPlan.getQueryId(), grpcAddr);
                    localQueries.emplace_back(*result, grpcAddr);
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

    auto id = getNextDistributedQueryId();
    state.queries.emplace(id, localQueries);
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

    for (const auto& localQuery : query)
    {
        try
        {
            const auto result = backend->start(localQuery);
            if (result)
            {
                NES_DEBUG("Starting query {} on node {} was successful.", localQuery.id, localQuery.grpcAddr);
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

    std::vector<LocalQueryStatus> localStatusResults;
    std::vector<Exception> exceptions;

    for (const auto& localQuery : query)
    {
        try
        {
            const auto result = backend->status(localQuery);
            if (result)
            {
                localStatusResults.emplace_back(*result);
            }
            else
            {
                exceptions.push_back(result.error());
            }
        }
        catch (std::exception& e)
        {
            exceptions.push_back(QueryStatusFailed("Message from external exception: {} ", e.what()));
        }
    }

    if (not exceptions.empty())
    {
        return std::unexpected{exceptions};
    }
    return DistributedQueryStatus{.localStatusSnapshots = localStatusResults, .queryId = queryId};
}

std::vector<DistributedQueryId> QueryManager::queries() const
{
    return state.queries | std::views::keys | std::ranges::to<std::vector>();
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
        | std::views::filter(
               [](auto idAndStatus)
               {
                   return idAndStatus->second.getGlobalQueryState() == QueryState::Started
                       || idAndStatus->second.getGlobalQueryState() == QueryState::Running;
               })
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

    for (const auto& localQuery : query)
    {
        try
        {
            auto result = backend->stop(localQuery);
            if (result)
            {
                NES_DEBUG("Stopping query {} on node {} was successful.", localQuery.id, localQuery.grpcAddr);
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

    for (const auto& localQuery : query)
    {
        try
        {
            auto result = backend->unregister(localQuery);
            if (result)
            {
                NES_DEBUG("Unregister of query {} on node {} was successful.", localQuery.id, localQuery.grpcAddr);
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
