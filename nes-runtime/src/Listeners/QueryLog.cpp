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

#include <Listeners/QueryLog.hpp>

#include <chrono>
#include <optional>
#include <ostream>
#include <ranges>
#include <string>
#include <utility>
#include <vector>

#include <bits/ranges_algo.h>
#include <magic_enum/magic_enum.hpp>

#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

QueryStateChange::QueryStateChange(Exception exception, std::chrono::system_clock::time_point timestamp)
    : state(QueryState::Failed), timestamp(timestamp), exception(exception)
{
}

inline std::ostream& operator<<(std::ostream& os, const QueryStateChange& statusChange)
{
    os << magic_enum::enum_name(statusChange.state) << " : " << std::chrono::system_clock::to_time_t(statusChange.timestamp);
    if (statusChange.exception.has_value())
    {
        os << " with exception: " + std::string(statusChange.exception.value().what());
    }
    return os;
}

bool QueryLog::logSourceTermination(LocalQueryId, OriginId, QueryTerminationType, std::chrono::system_clock::time_point)
{
    /// TODO #34: part of redesign of single node worker
    return true; /// nop
}

bool QueryLog::logQueryFailure(LocalQueryId queryId, Exception exception, std::chrono::system_clock::time_point timestamp)
{
    QueryStateChange statusChange(std::move(exception), timestamp);

    const auto log = queryStatusLog.wlock();
    if (log->contains(queryId))
    {
        auto& changes = (*log)[queryId];
        const auto pos = std::ranges::upper_bound(
            changes, statusChange, [](const QueryStateChange& lhs, const QueryStateChange& rhs) { return lhs.timestamp < rhs.timestamp; });
        changes.emplace(pos, std::move(statusChange));
        return true;
    }
    return false;
}

bool QueryLog::logQueryStatusChange(LocalQueryId queryId, QueryState status, std::chrono::system_clock::time_point timestamp)
{
    QueryStateChange statusChange(std::move(status), timestamp);

    const auto log = queryStatusLog.wlock();
    auto& changes = (*log)[queryId];
    const auto pos = std::ranges::upper_bound(
        changes, statusChange, [](const QueryStateChange& lhs, const QueryStateChange& rhs) { return lhs.timestamp < rhs.timestamp; });
    changes.emplace(pos, std::move(statusChange));
    return true;
}

std::optional<QueryLog::Log> QueryLog::getLogForQuery(LocalQueryId queryId) const
{
    const auto log = queryStatusLog.rlock();
    if (const auto it = log->find(queryId); it != log->end())
    {
        return it->second;
    }
    return std::nullopt;
}

namespace
{
std::optional<LocalQueryStatus> getQuerySummaryImpl(const auto& log, LocalQueryId queryId)
{
    if (const auto queryLog = log->find(queryId); queryLog != log->end())
    {
        /// Unfortunately the multithreaded nature of the query engine cannot guarantee event ordering.
        /// We handle out-of-order events by keeping the most recent timestamp for each event type.
        /// Final state is determined by priority: Failed > Stopped > Running > Started > Registered.
        LocalQueryStatus status;
        status.queryId = queryId;

        for (const auto& statusChange : queryLog->second)
        {
            switch (statusChange.state)
            {
                case QueryState::Failed:
                    status.metrics.stop = statusChange.timestamp;
                    status.metrics.error = statusChange.exception;
                    break;
                case QueryState::Stopped:
                    status.metrics.stop = statusChange.timestamp;
                    break;
                case QueryState::Started:
                    status.metrics.start = statusChange.timestamp;
                    break;
                case QueryState::Running:
                    status.metrics.running = statusChange.timestamp;
                    break;
                default: /// QueryState::Registered
                    break; /// noop
            }
        }

        /// Determine state based on available metrics and timestamps
        status.state = status.metrics.error.has_value() ? QueryState::Failed
            : status.metrics.stop.has_value()           ? QueryState::Stopped
            : status.metrics.running.has_value()        ? QueryState::Running
            : status.metrics.start.has_value()          ? QueryState::Started
                                                        : QueryState::Registered;

        return status;
    }
    return std::nullopt;
}
}

std::optional<LocalQueryStatus> QueryLog::getQuerySummary(const LocalQueryId queryId) const
{
    const auto log = queryStatusLog.rlock();
    return getQuerySummaryImpl(log, queryId);
}

std::vector<LocalQueryStatus> QueryLog::getSummary() const
{
    const auto queryStatusLogLocked = queryStatusLog.rlock();
    std::vector<LocalQueryStatus> summaries;
    summaries.reserve(queryStatusLogLocked->size());
    for (const auto id : std::views::keys(*queryStatusLogLocked))
    {
        summaries.emplace_back(getQuerySummaryImpl(queryStatusLogLocked, id).value());
    }
    return summaries;
}
}
