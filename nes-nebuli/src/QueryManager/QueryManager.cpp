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
#include <cstddef>
#include <exception>
#include <optional>
#include <ranges>
#include <thread>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Pointers.hpp>
#include <fmt/chrono.h>
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

std::expected<LocalQueryId, Exception> QueryManager::registerQuery(const LogicalPlan& plan)
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

std::expected<void, Exception> QueryManager::start(LocalQueryId queryId)
{
    auto queryResult = getQuery(queryId);
    if (!queryResult.has_value())
    {
        return std::unexpected(queryResult.error());
    }

    try
    {
        std::chrono::system_clock::time_point queryStartTimestamp = std::chrono::system_clock::now();
        if (const auto startResult = backend->start(queryId); !startResult)
        {
            return std::unexpected{startResult.error()};
        }

        /// The query is expected to be moved into the started state pretty quickly after lowering, it is very unlikely to even observe
        /// the status not changing immediatly, so a rapid polling interval is appropriate.
        constexpr auto statusPollInterval = std::chrono::milliseconds(10);
        constexpr size_t statusRetries = 14;
        NES_DEBUG("Starting query {} was successful. Waiting for state to change", queryId);
        for (size_t i = 0; i < statusRetries; ++i)
        {
            const auto result = backend->status(queryId);
            if (!result)
            {
                return std::unexpected{QueryStartFailed(
                    "Checking status, while waiting for query state to change encountered an exception: {}", result.error().what())};
            }
            /// Waiting until the query state changed. Even if the query status changes to failed we consider the start to be successful.
            /// Subsequent status requests will find the query in a failed state.
            if (result->state != QueryState::Registered)
            {
                NES_DEBUG(
                    "Query {} started successfully after {}.",
                    queryId,
                    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - queryStartTimestamp));
                return {};
            }
            std::this_thread::sleep_for(statusPollInterval * std::pow(2, i));
        }

        return std::unexpected{QueryStartFailed("Query state remains `Registered` after {} retries", statusRetries)};
    }
    catch (std::exception& e)
    {
        return std::unexpected{QueryStartFailed("Message from external exception: {} ", e.what())};
    }
}

std::expected<LocalQueryStatus, Exception> QueryManager::status(LocalQueryId queryId) const
{
    return getQuery(queryId).and_then([this](auto validatedQueryId) { return backend->status(validatedQueryId); });
}

std::vector<LocalQueryId> QueryManager::queries() const
{
    return state.queries | std::ranges::to<std::vector>();
}

std::expected<WorkerStatus, Exception> QueryManager::workerStatus(std::chrono::system_clock::time_point after) const
{
    return backend->workerStatus(after);
}

std::vector<LocalQueryId> QueryManager::getRunningQueries() const
{
    return state.queries
        | std::views::transform(
               [this](const auto& id) -> std::optional<std::pair<LocalQueryId, LocalQueryStatus>>
               {
                   auto result = status(id);
                   if (result)
                   {
                       return std::optional<std::pair<LocalQueryId, LocalQueryStatus>>{{id, *result}};
                   }
                   return std::nullopt;
               })
        | std::views::filter([](const auto& idAndStatus) { return idAndStatus.has_value(); })
        | std::views::filter(
               [](auto idAndStatus)
               { return idAndStatus->second.state == QueryState::Started || idAndStatus->second.state == QueryState::Running; })
        | std::views::transform([](auto idAndStatus) { return idAndStatus->first; }) | std::ranges::to<std::vector>();
}

std::expected<LocalQueryId, Exception> QueryManager::getQuery(LocalQueryId query) const
{
    if (state.queries.contains(query))
    {
        return query;
    }
    return std::unexpected(QueryNotFound("Query {} not found", query));
}

std::expected<void, Exception> QueryManager::stop(LocalQueryId queryId)
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

std::expected<void, Exception> QueryManager::unregister(LocalQueryId queryId)
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
            auto erased = state.queries.erase(queryId);
            INVARIANT(erased == 1, "Should not unregister query that has not been registered");
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
