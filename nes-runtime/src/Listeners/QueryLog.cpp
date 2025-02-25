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

#include <mutex>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <magic_enum.hpp>

namespace NES::Runtime
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

bool QueryLog::logSourceTermination(QueryId, OriginId, QueryTerminationType)
{
    /// TODO #34: part of redesign of single node worker
    return true; /// nop
}

bool QueryLog::logQueryFailure(QueryId queryId, Exception exception)
{
    auto writeLock = queryStatusLog.wlock();
    if (writeLock->contains(queryId))
    {
        (*writeLock)[queryId].emplace_back(exception);
        return true;
    }
    return false;
}

bool QueryLog::logQueryStatusChange(QueryId queryId, Runtime::Execution::QueryStatus status)
{
    auto writeLock = queryStatusLog.wlock();
    (*writeLock)[queryId].emplace_back(status);
    return true;
}

std::optional<QueryLog::Log> QueryLog::getLogForQuery(QueryId queryId)
{
    auto log = queryStatusLog.rlock();
    const auto result = log->find(queryId);
    if (result != log->end())
    {
        return result->second;
    }
    return std::nullopt;
}

std::optional<QuerySummary> QueryLog::getQuerySummary(QueryId queryId)
{
    auto readLock = queryStatusLog.rlock();
    const auto log = readLock->find(queryId);
    if (log != readLock->end())
    {
        QuerySummary summary = {queryId, Execution::QueryStatus::Registered, 0, {}};
        summary.currentStatus = log->second.back().state; /// last status change

        Execution::QueryStatus previousState = Execution::QueryStatus::Registered;
        for (const auto& statusChange : log->second)
        {
            if (statusChange.state == Execution::QueryStatus::Failed)
            {
                summary.exceptions.push_back(statusChange.exception.value());
            }
            if (Execution::QueryStatusUtil::hasRestarted(previousState, statusChange.state))
            {
                ++(summary.numberOfRestarts);
            }
            previousState = statusChange.state;
        }
        return summary;
    }
    return std::nullopt;
}
}
