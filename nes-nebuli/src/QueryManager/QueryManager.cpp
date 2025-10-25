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
#include <exception>
#include <optional>
#include <ranges>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Pointers.hpp>
#include <ErrorHandling.hpp>
#include <WorkerStatus.hpp>

namespace NES
{

QueryManager::QueryManager(UniquePtr<QuerySubmissionBackend> backend, QueryManagerState state)
    : state(std::move(state)), backend(std::move(backend))
{
}

QueryManager::QueryManager(UniquePtr<QuerySubmissionBackend> backend) : backend(std::move(backend))
{
}

std::expected<QueryId, Exception> QueryManager::registerQuery(const LogicalPlan& plan)
{
    try
    {
        const auto result = backend->registerQuery(plan);
        if (result)
        {
            NES_DEBUG("Registration of local query {} was successful.", *result);
            state.queries.emplace(*result);
            return *result;
        }
        return std::unexpected{result.error()};
    }
    catch (const std::exception& e)
    {
        return std::unexpected{QueryRegistrationFailed("Message from external exception: {}", e.what())};
    }
}

std::expected<void, Exception> QueryManager::start(QueryId queryId)
{
    auto queryResult = getQuery(queryId);
    if (!queryResult.has_value())
    {
        return std::unexpected(queryResult.error());
    }

    try
    {
        if (const auto startResult = backend->start(queryId); !startResult)
        {
            return std::unexpected{startResult.error()};
        }

        NES_DEBUG("Starting query {} was successful. Waiting for state to change", queryId);
        while (true)
        {
            const auto result = backend->status(queryId);
            if (!result)
            {
                return std::unexpected{QueryStartFailed(
                    "Checking status, while waiting for query state to change encountered an exception: {}", result.error().what())};
            }

            if (result->state != QueryState::Registered)
            {
                break;
            }
        }

        NES_DEBUG("Query {} started successfully.", queryId);
        return {};
    }
    catch (std::exception& e)
    {
        return std::unexpected{QueryStartFailed("Message from external exception: {} ", e.what())};
    }
}

std::expected<LocalQueryStatus, Exception> QueryManager::status(QueryId queryId) const
{
    return getQuery(queryId).and_then([this](auto validatedQueryId) { return backend->status(validatedQueryId); });
}

std::vector<QueryId> QueryManager::queries() const
{
    return state.queries | std::ranges::to<std::vector>();
}

std::expected<WorkerStatus, Exception> QueryManager::workerStatus(std::chrono::system_clock::time_point after) const
{
    return backend->workerStatus(after);
}

std::vector<QueryId> QueryManager::getRunningQueries() const
{
    return state.queries
        | std::views::transform(
               [this](const auto& id) -> std::optional<std::pair<QueryId, LocalQueryStatus>>
               {
                   auto result = status(id);
                   if (result)
                   {
                       return std::optional<std::pair<QueryId, LocalQueryStatus>>{{id, *result}};
                   }
                   return std::nullopt;
               })
        | std::views::filter([](const auto& idAndStatus) { return idAndStatus.has_value(); })
        | std::views::filter(
               [](auto idAndStatus)
               { return idAndStatus->second.state == QueryState::Started || idAndStatus->second.state == QueryState::Running; })
        | std::views::transform([](auto idAndStatus) { return idAndStatus->first; }) | std::ranges::to<std::vector>();
}

std::expected<QueryId, Exception> QueryManager::getQuery(QueryId query) const
{
    if (state.queries.contains(query))
    {
        return query;
    }
    return std::unexpected(QueryNotFound("Query {} not found", query));
}

std::expected<void, Exception> QueryManager::stop(QueryId queryId)
{
    auto queryResult = getQuery(queryId);
    if (!queryResult.has_value())
    {
        return std::unexpected(queryResult.error());
    }

    try
    {
        auto result = backend->stop(queryId);
        if (result)
        {
            NES_DEBUG("Stopping query {} was successful.", queryId);
            return {};
        }
        return std::unexpected{result.error()};
    }
    catch (std::exception& e)
    {
        return std::unexpected{QueryStopFailed("Message from external exception: {} ", e.what())};
    }
}

std::expected<void, Exception> QueryManager::unregister(QueryId queryId)
{
    auto queryResult = getQuery(queryId);
    if (!queryResult.has_value())
    {
        return std::unexpected(queryResult.error());
    }

    try
    {
        auto result = backend->unregister(queryId);
        if (result)
        {
            state.queries.erase(queryId);
            NES_DEBUG("Unregister of query {} was successful.", queryId);
            return {};
        }
        return std::unexpected{result.error()};
    }
    catch (std::exception& e)
    {
        return std::unexpected{QueryUnregistrationFailed("Message from external exception: {} ", e.what())};
    }
}

}
