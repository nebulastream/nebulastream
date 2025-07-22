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

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <optional>
#include <ostream>
#include <ranges>
#include <string>
#include <utility>
#include <vector>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <magic_enum/magic_enum.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

inline std::ostream& operator<<(std::ostream& os, const QueryStatusChange& statusChange)
{
    os << magic_enum::enum_name(statusChange.state) << " : " << std::chrono::system_clock::to_time_t(statusChange.timestamp);
    if (statusChange.exception.has_value())
    {
        os << " with exception: " + std::string(statusChange.exception.value().what());
    }
    return os;
}

bool QueryLog::logSourceTermination(QueryId, OriginId, QueryTerminationType, std::chrono::system_clock::time_point)
{
    /// TODO #34: part of redesign of single node worker
    return true; /// nop
}

bool QueryLog::logQueryFailure(QueryId queryId, Exception exception, std::chrono::system_clock::time_point timestamp)
{
    QueryStatusChange statusChange(std::move(exception), timestamp);

    const auto log = queryStatusLog.wlock();
    if (log->contains(queryId))
    {
        auto& changes = (*log)[queryId];
        const auto pos = std::ranges::upper_bound(
            changes,
            statusChange,
            [](const QueryStatusChange& lhs, const QueryStatusChange& rhs) { return lhs.timestamp < rhs.timestamp; });
        changes.emplace(pos, std::move(statusChange));
        return true;
    }
    return false;
}

bool QueryLog::logQueryStatusChange(QueryId queryId, QueryStatus status, std::chrono::system_clock::time_point timestamp)
{
    QueryStatusChange statusChange(std::move(status), timestamp);

    const auto log = queryStatusLog.wlock();
    auto& changes = (*log)[queryId];
    const auto pos = std::ranges::upper_bound(
        changes, statusChange, [](const QueryStatusChange& lhs, const QueryStatusChange& rhs) { return lhs.timestamp < rhs.timestamp; });
    changes.emplace(pos, std::move(statusChange));
    return true;
}

std::optional<QueryLog::Log> QueryLog::getLogForQuery(QueryId queryId)
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
std::optional<QuerySummary> getQuerySummaryImpl(const auto& log, QueryId queryId)
{
    if (const auto queryLog = log->find(queryId); queryLog != log->end())
    {
        /// Unfortunatly the multithreaded nature of the query engine cannot guarantee that a `Running` event always comes before the `Stopped` event.
        /// For now, we count the number of Running and Stopped events to determine the number of restarts and if the query is currently running.
        /// Failures are counting towards the number of stops. If a query has equally (or more) Stopped than Running events, the QueryLog
        /// assumes it to be in the Stopped state.
        std::vector<QueryRunSummary> runs;
        for (const auto& statusChange : queryLog->second)
        {
            if (statusChange.state == QueryStatus::Failed)
            {
                if (runs.empty() || runs.back().stop)
                {
                    runs.emplace_back();
                }
                runs.back().stop = statusChange.timestamp;
                runs.back().error = statusChange.exception;
            }
            else if (statusChange.state == QueryStatus::Stopped)
            {
                if (runs.empty() || runs.back().stop)
                {
                    runs.emplace_back();
                }
                runs.back().stop = statusChange.timestamp;
            }
            else if (statusChange.state == QueryStatus::Started)
            {
                if (runs.empty() || runs.back().start)
                {
                    runs.emplace_back();
                }
                runs.back().start = statusChange.timestamp;
            }
            else if (statusChange.state == QueryStatus::Running)
            {
                if (runs.empty() || runs.back().running)
                {
                    runs.emplace_back();
                }
                runs.back().running = statusChange.timestamp;
            }
        }

        QuerySummary summary = {.queryId = queryId, .currentStatus = QueryStatus::Registered, .runs = std::move(runs)};

        if (summary.runs.empty())
        {
            summary.currentStatus = QueryStatus::Registered;
        }
        else if (!summary.runs.back().stop)
        {
            summary.currentStatus = QueryStatus::Running;
        }
        else if (summary.runs.back().error)
        {
            summary.currentStatus = QueryStatus::Failed;
        }
        else
        {
            summary.currentStatus = QueryStatus::Stopped;
        }

        return summary;
    }
    return std::nullopt;
}
}

std::optional<QuerySummary> QueryLog::getQuerySummary(QueryId queryId)
{
    const auto log = queryStatusLog.rlock();
    return getQuerySummaryImpl(log, queryId);
}
std::vector<QuerySummary> QueryLog::getSummary()
{
    auto queryStatusLogLocked = queryStatusLog.rlock();
    std::vector<QuerySummary> summaries;
    summaries.reserve(queryStatusLogLocked->size());
    for (auto id : std::views::keys(*queryStatusLogLocked))
    {
        summaries.emplace_back(getQuerySummaryImpl(queryStatusLogLocked, id).value());
    }
    return summaries;
}
}
