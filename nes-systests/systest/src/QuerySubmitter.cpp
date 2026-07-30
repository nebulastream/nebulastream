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
#include <ranges>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <QueryManager/QueryManager.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <DistributedLogicalPlan.hpp>
#include <DistributedQuery.hpp>
#include <ErrorHandling.hpp>

namespace NES::Systest
{
namespace
{
bool isBackendError(const ErrorCode code)
{
    return code == ErrorCode::QueryStartFailed || code == ErrorCode::QueryStopFailed || code == ErrorCode::QueryStatusFailed
        || code == ErrorCode::QueryNotFound || code == ErrorCode::CannotSerialize || code == ErrorCode::UnknownException;
}

Exception combineErrors(std::vector<Exception> errors)
{
    INVARIANT(!errors.empty(), "Cannot combine an empty error list");
    const auto selected = std::ranges::find_if(errors, [](const Exception& error) { return isBackendError(error.code()); });
    const auto code = selected == errors.end() ? errors.front().code() : selected->code();
    return Exception{
        fmt::format("{}", fmt::join(errors | std::views::transform([](const Exception& error) { return error.what(); }), "\n")), code};
}

std::vector<Exception> statusRetrievalErrors(const DistributedQueryStatusSnapshot& status)
{
    std::vector<Exception> errors;
    for (const auto& [host, localStatuses] : status.localStatusSnapshots)
    {
        for (const auto& [localQueryId, localStatus] : localStatuses)
        {
            if (!localStatus)
            {
                errors.emplace_back(
                    fmt::format("Query {} local query {} on {}: {}", status.queryId, localQueryId, host, localStatus.error().what()),
                    localStatus.error().code());
            }
        }
    }
    return errors;
}

std::vector<Exception> queryStatusErrors(const DistributedQueryId& queryId, std::vector<Exception> errors)
{
    for (auto& error : errors)
    {
        error = Exception{fmt::format("Query {}: {}", queryId, error.what()), error.code()};
    }
    return errors;
}
}

QuerySubmitter::QuerySubmitter(std::unique_ptr<QueryManager> queryManager) : queryManager(std::move(queryManager))
{
}

std::expected<DistributedQueryId, Exception> QuerySubmitter::startQuery(const DistributedLogicalPlan& plan)
{
    return startQuery(plan, std::chrono::steady_clock::time_point::max(), {}, std::chrono::seconds(5));
}

std::expected<DistributedQueryId, Exception> QuerySubmitter::startQuery(
    const DistributedLogicalPlan& plan, const std::chrono::steady_clock::time_point deadline, const std::stop_token stopToken)
{
    return startQuery(plan, deadline, stopToken, std::chrono::seconds(5));
}

std::expected<DistributedQueryId, Exception> QuerySubmitter::startQuery(
    const DistributedLogicalPlan& plan,
    const std::chrono::steady_clock::time_point deadline,
    const std::stop_token stopToken,
    const std::chrono::milliseconds cancellationGracePeriod)
{
    /// Make sure the queryplan is passed through serialization logic.
    std::unordered_map<Host, std::vector<std::string>> serializationErrorsPerWorker;
    for (const auto& [grpc, localPlans] : plan)
    {
        for (const auto& localPlan : localPlans)
        {
            const auto serialized = QueryPlanSerializationUtil::serializeQueryPlan(localPlan);
            const auto deserialized = QueryPlanSerializationUtil::deserializeQueryPlan(serialized);
            if (deserialized != localPlan)
            {
                serializationErrorsPerWorker[grpc].emplace_back(fmt::format(
                    "Query plan serialization is wrong: plan != deserialize(serialize(plan)), with plan:\n{} and "
                    "deserialize(serialize(plan)):\n{}",
                    explain(localPlan, ExplainVerbosity::Debug),
                    explain(deserialized, ExplainVerbosity::Debug)));
            }
        }
    }

    if (!serializationErrorsPerWorker.empty())
    {
        return std::unexpected(CannotSerialize("Encountered serialization errors: {}", serializationErrorsPerWorker));
    }

    auto result = queryManager->start(plan, deadline, stopToken);

    if (!result.has_value())
    {
        auto errors = std::move(result.error());
        const auto cleanupDeadline = std::chrono::steady_clock::now() + cancellationGracePeriod;
        if (auto cleaned = queryManager->cleanupPending(cleanupDeadline); !cleaned)
        {
            auto cleanupErrors = std::move(cleaned.error());
            errors.insert(errors.end(), cleanupErrors.begin(), cleanupErrors.end());
            failedCleanupDeadline = cleanupDeadline;
        }
        return std::unexpected(combineErrors(std::move(errors)));
    }

    ids.emplace(*result);
    return result.value();
}

void QuerySubmitter::stopQuery(const DistributedQueryId& query)
{
    stopQuery(query, std::chrono::steady_clock::time_point::max(), {});
}

void QuerySubmitter::stopQuery(
    const DistributedQueryId& query, const std::chrono::steady_clock::time_point deadline, const std::stop_token stopToken)
{
    if (auto stopped = queryManager->stop(query, deadline, stopToken); !stopped.has_value())
    {
        throw combineErrors(std::move(stopped.error()));
    }
    ids.erase(query);
}

void QuerySubmitter::cleanupQuery(const DistributedQueryId& query)
{
    cleanupQuery(query, std::chrono::steady_clock::time_point::max(), {});
}

void QuerySubmitter::cleanupQuery(
    const DistributedQueryId& query, const std::chrono::steady_clock::time_point deadline, const std::stop_token stopToken)
{
    if (auto cleaned = queryManager->cleanup(query, deadline, stopToken); !cleaned)
    {
        failedCleanupDeadline = deadline;
        throw combineErrors(std::move(cleaned.error()));
    }
    ids.erase(query);
}

void QuerySubmitter::releaseQuery(const DistributedQueryId& query)
{
    queryManager->release(query);
    ids.erase(query);
}

void QuerySubmitter::cleanup(const std::chrono::steady_clock::time_point deadline)
{
    const auto effectiveDeadline = failedCleanupDeadline ? std::min(deadline, *failedCleanupDeadline) : deadline;
    if (auto cleaned = queryManager->cleanupPending(effectiveDeadline); !cleaned)
    {
        throw combineErrors(std::move(cleaned.error()));
    }
    failedCleanupDeadline.reset();
}

void QuerySubmitter::shutdown(const std::chrono::steady_clock::time_point deadline)
{
    queryManager->shutdown(failedCleanupDeadline ? std::min(deadline, *failedCleanupDeadline) : deadline);
}

std::optional<std::chrono::steady_clock::time_point> QuerySubmitter::failedTeardownDeadline() const
{
    return failedCleanupDeadline;
}

DistributedQueryStatusSnapshot QuerySubmitter::waitForQueryTermination(const DistributedQueryId& query)
{
    while (true)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
        auto queryStatus = queryManager->status(query);
        if (!queryStatus.has_value())
        {
            throw combineErrors(queryStatusErrors(query, std::move(queryStatus.error())));
        }
        if (auto errors = statusRetrievalErrors(*queryStatus); !errors.empty())
        {
            throw combineErrors(std::move(errors));
        }
        if (queryStatus->getGlobalQueryStatus() == DistributedQueryStatus::Stopped
            || queryStatus->getGlobalQueryStatus() == DistributedQueryStatus::Failed)
        {
            return *queryStatus;
        }
    }
}

std::vector<DistributedQueryStatusSnapshot> QuerySubmitter::pollFinishedQueries()
{
    return pollFinishedQueries(std::chrono::steady_clock::time_point::max(), {});
}

std::vector<DistributedQueryStatusSnapshot>
QuerySubmitter::pollFinishedQueries(const std::chrono::steady_clock::time_point deadline, const std::stop_token stopToken)
{
    auto results = pollFinishedQueriesRetained(deadline, stopToken);
    for (const auto& result : results)
    {
        ids.erase(result.queryId);
    }
    return results;
}

std::vector<DistributedQueryStatusSnapshot>
QuerySubmitter::pollFinishedQueriesRetained(const std::chrono::steady_clock::time_point deadline, const std::stop_token stopToken)
{
    std::vector<DistributedQueryStatusSnapshot> results;
    for (const auto& id : ids)
    {
        auto queryStatus = queryManager->status(id, deadline, stopToken);
        if (!queryStatus.has_value())
        {
            throw combineErrors(queryStatusErrors(id, std::move(queryStatus.error())));
        }
        if (auto errors = statusRetrievalErrors(*queryStatus); !errors.empty())
        {
            throw combineErrors(std::move(errors));
        }
        if (queryStatus->getGlobalQueryStatus() == DistributedQueryStatus::Stopped
            || queryStatus->getGlobalQueryStatus() == DistributedQueryStatus::Failed)
        {
            results.push_back(std::move(*queryStatus));
        }
    }
    return results;
}

std::vector<DistributedQueryStatusSnapshot> QuerySubmitter::finishedQueries()
{
    while (true)
    {
        auto results = pollFinishedQueries();
        if (!results.empty())
        {
            return results;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
}
}
