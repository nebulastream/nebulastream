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

#include <QuerySubmitter.hpp>

#include <chrono>
#include <memory>
#include <thread>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>

#include <Plans/LogicalPlan.hpp>
#include <QueryManager/QueryManager.hpp>
#include <ErrorHandling.hpp>
#include <SystestState.hpp>

namespace NES::Systest
{

QuerySubmitter::QuerySubmitter(std::unique_ptr<QueryManager> queryManager) : queryManager(std::move(queryManager))
{
}

std::expected<QueryId, Exception> QuerySubmitter::registerQuery(const LogicalPlan& plan)
{
    /// Make sure the queryplan is passed through serialization logic.
    const auto serialized = QueryPlanSerializationUtil::serializeQueryPlan(plan);
    const auto deserialized = QueryPlanSerializationUtil::deserializeQueryPlan(serialized);
    if (deserialized == plan)
    {
        return queryManager->registerQuery(deserialized);
    }
    const auto exception = CannotSerialize(
        "Query plan serialization is wrong: plan != deserialize(serialize(plan)), with plan:\n{} and deserialize(serialize(plan)):\n{}",
        explain(plan, ExplainVerbosity::Debug),
        explain(deserialized, ExplainVerbosity::Debug));
    return std::unexpected(exception);
}

void QuerySubmitter::startQuery(QueryId query)
{
    if (auto started = queryManager->start(query); !started.has_value())
    {
        throw started.error();
    }
    ids.emplace(query);
}

void QuerySubmitter::stopQuery(const QueryId query)
{
    if (auto stopped = queryManager->stop(query); !stopped.has_value())
    {
        throw stopped.error();
    }
}

void QuerySubmitter::unregisterQuery(const QueryId query)
{
    if (auto unregistered = queryManager->unregister(query); !unregistered.has_value())
    {
        throw unregistered.error();
    }
}

QuerySummary QuerySubmitter::waitForQueryTermination(const QueryId query)
{
    while (true)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
        if (const auto summary = queryManager->status(query))
        {
            if (summary->currentStatus == QueryStatus::Stopped)
            {
                return *summary;
            }
        }
    }
}

std::vector<QuerySummary> QuerySubmitter::finishedQueries()
{
    while (true)
    {
        std::vector<QuerySummary> results;
        for (const auto id : ids)
        {
            if (auto summary = queryManager->status(id))
            {
                if (summary->currentStatus == QueryStatus::Failed || summary->currentStatus == QueryStatus::Stopped)
                {
                    results.emplace_back(std::move(*summary));
                }
            }
        }
        if (results.empty())
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
            continue;
        }

        for (auto& result : results)
        {
            ids.erase(result.queryId);
        }

        return results;
    }
}
}
