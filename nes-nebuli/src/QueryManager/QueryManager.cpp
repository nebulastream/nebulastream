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

QueryStatusNotifier::QueryStatusNotifier(QueryManager& owner) : owner(owner)
{
    thread = Thread(
        "QueryStatusNotifier",
        [this](std::stop_token token)
        {
            std::chrono::time_point<std::chrono::system_clock> nextUpdate{std::chrono::microseconds(0)};
            while (!token.stop_requested())
            {
                auto lastUpdate = std::exchange(nextUpdate, std::chrono::system_clock::now());
                auto result = this->owner.workerStatus(lastUpdate);
                auto state = this->notifiers.wlock();
                if (result)
                {
                    for (const auto& activeQuery : result->activeQueries)
                    {
                        auto it = state->find({activeQuery.queryId, QueryState::Running});
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
                    for (const auto& terminatedQuery : result->terminatedQueries)
                    {
                        auto it = state->find({terminatedQuery.queryId, QueryState::Stopped});
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
        });
}

QueryManager::QueryManager(UniquePtr<QuerySubmissionBackend> backend, QueryManagerState state)
    : state(std::move(state)), backend(std::move(backend)), notifier(*this)
{
}

QueryManager::QueryManager(UniquePtr<QuerySubmissionBackend> backend) : backend(std::move(backend)), notifier(*this)
{
}

std::expected<LocalQueryId, Exception> QueryManager::registerQuery(const PlanStage::OptimizedLogicalPlan& plan)
{
    try
    {
        const auto result = backend->registerQuery(plan.plan);
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
        const auto result = backend->start(queryId);
        if (result)
        {
            NES_DEBUG("Starting query {} was successful.", queryId);
            return {};
        }
        return std::unexpected{result.error()};
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
        | std::views::filter([](auto idAndStatus) { return idAndStatus.has_value(); })
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
