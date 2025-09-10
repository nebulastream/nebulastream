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

#include <exception>
#include <expected>
#include <utility>
#include <vector>

#include <Listeners/QueryLog.hpp>
#include <Util/Pointers.hpp>
#include <DistributedQueryId.hpp>
#include <ErrorHandling.hpp>
#include <QueryPlanning.hpp>

namespace NES
{
QueryManager::QueryManager(UniquePtr<QuerySubmissionBackend> backend) : backend(std::move(backend))
{
}

std::expected<Query, Exception> QueryManager::registerQuery(PlanStage::DistributedLogicalPlan&& plan)
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
    return Query{localQueries, std::move(plan)};
}

std::expected<void, Exception> QueryManager::start(const Query& query)
{
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

            return std::unexpected{result.error()};
        }
        catch (std::exception& e)
        {
            return std::unexpected{QueryStartFailed("Message from external exception: {} ", e.what())};
        }
    }
    return {};
}

std::expected<DistributedQueryStatus, std::vector<Exception>> QueryManager::status(const Query& query) const
{
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
    return DistributedQueryStatus{.localStatusSnapshots = localStatusResults};
}

std::expected<void, std::vector<Exception>> QueryManager::stop(const Query& query)
{
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

std::expected<void, std::vector<Exception>> QueryManager::unregister(const Query& query)
{
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
